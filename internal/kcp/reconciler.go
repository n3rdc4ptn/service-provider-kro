package kcp

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/kustomize"
	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	apiv1alpha1 "github.com/openmcp-project/service-provider-kro/api/v1alpha1"
)

const (
	ociRepositoryName = "kro-oci-repository"
	helmReleaseName   = "kro-helm-release"
	crdHelmReleaseName = "kro-crds-helm-release"
	kroSystemNamespace  = "kro-system"
	defaultReleaseName = "kro"

	kubeconfigMountPath = "/etc/kro/kubeconfig"
	kubeconfigKey       = "kubeconfig"
	kubeconfigSecretFmt = "kcp-%s-kubeconfig"

)

// KCPReconciler reconciles KroVersionRequest objects across consumer workspaces.
type KCPReconciler struct {
	// Manager is the multicluster manager providing per-cluster access.
	Manager mcmanager.Manager
	// PlatformClient talks to the platform cluster where Flux resources live.
	PlatformClient client.Client
	// PodNamespace is the controller's own namespace on the platform cluster.
	PodNamespace string
	// ProviderConfig holds the current provider configuration.
	ProviderConfig *apiv1alpha1.ProviderConfig
	// KCPServerURL is the kcp API server URL for building kubeconfigs.
	KCPServerURL string
	// KCPConfig is the REST config for the kcp API server (used for TokenRequest).
	KCPConfig *rest.Config
}

// Reconcile handles a KroVersionRequest event from a consumer workspace.
func (r *KCPReconciler) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
	clusterName := string(req.ClusterName)
	l := logf.FromContext(ctx).WithValues("cluster", clusterName, "name", req.Name)
	l.Info("reconciling KroVersionRequest")

	cl, err := r.clusterClient(ctx, req.ClusterName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("getting cluster client: %w", err)
	}

	// Read the KroVersionRequest from the workspace
	kvr := &unstructured.Unstructured{}
	kvr.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   apiv1alpha1.GroupVersion.Group,
		Version: apiv1alpha1.GroupVersion.Version,
		Kind:    "KroVersionRequest",
	})
	if err := cl.Get(ctx, client.ObjectKey{Name: req.Name}, kvr); err != nil {
		if apierrors.IsNotFound(err) {
			l.Info("KroVersionRequest deleted, cleaning up")
			return r.handleDeletion(ctx, clusterName, cl)
		}
		return ctrl.Result{}, fmt.Errorf("getting KroVersionRequest: %w", err)
	}

	if kvr.GetDeletionTimestamp() != nil {
		l.Info("KroVersionRequest being deleted, cleaning up")
		return r.handleDeletion(ctx, clusterName, cl)
	}

	// Enforce singleton: only the oldest KroVersionRequest per workspace is active
	if isOldest, err := r.isOldestInWorkspace(ctx, cl, kvr); err != nil {
		return ctrl.Result{}, err
	} else if !isOldest {
		l.Info("rejecting duplicate KroVersionRequest, not the oldest")
		return ctrl.Result{}, r.setStatus(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseFailed,
			"Only one KroVersionRequest per workspace is allowed. This is not the oldest.")
	}

	// Extract version from spec
	spec, _, _ := unstructured.NestedMap(kvr.Object, "spec")
	requestedVersion, _ := spec["version"].(string)
	if requestedVersion == "" {
		return ctrl.Result{}, r.setStatus(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseFailed, "spec.version is required")
	}

	// Validate version against ProviderConfig
	kroVersion, err := r.ProviderConfig.ResolveVersion(requestedVersion)
	if err != nil {
		availableVersions := r.availableVersions()
		_ = r.setStatusWithVersions(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseFailed, err.Error(), availableVersions)
		return ctrl.Result{}, nil // don't requeue, user error
	}

	// Ensure SA + RBAC in workspace, request token only if needed
	ns := tenantNamespace(clusterName)
	serverURL := r.workspaceServerURL(clusterName)

	// Ensure platform namespace
	if err := ensurePlatformNamespace(ctx, r.PlatformClient, ns); err != nil {
		return ctrl.Result{}, err
	}

	// Check if we already have a valid token stored on the platform
	existingToken, existingExpiry := r.readExistingToken(ctx, ns, clusterName)
	var tokenExpiry time.Time

	if existingToken != "" && !IsTokenExpiring(existingExpiry) {
		// Token is still valid, reuse it
		tokenExpiry = existingExpiry
	} else {
		// Need a fresh token — ensure SA + RBAC and request one
		tokenResult, err := EnsureWorkspaceToken(ctx, cl, r.KCPConfig)
		if err != nil {
			_ = r.setStatus(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseFailed, fmt.Sprintf("Token provisioning failed: %v", err))
			return ctrl.Result{}, err
		}
		l.Info("token issued", "expiration", tokenResult.ExpirationTimestamp)
		tokenExpiry = tokenResult.ExpirationTimestamp

		// Store kubeconfig with fresh token on platform
		kubeconfigData := BuildKubeconfig(serverURL, tokenResult.Token, nil)
		if err := r.ensureKubeconfigSecret(ctx, ns, clusterName, kubeconfigData, tokenExpiry); err != nil {
			_ = r.setStatus(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseFailed, fmt.Sprintf("Kubeconfig secret failed: %v", err))
			return ctrl.Result{}, err
		}
	}

	// Ensure workload cluster access (for kro controller deployment)
	workloadAccess, err := EnsureWorkloadAccess(ctx, r.PlatformClient, ns, clusterName)
	if err != nil {
		_ = r.setStatus(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseFailed, fmt.Sprintf("Workload access failed: %v", err))
		return ctrl.Result{}, err
	}
	if !workloadAccess.Granted {
		l.Info("workload cluster not yet ready, requeuing")
		_ = r.setStatus(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseProvisioning, "Waiting for workload cluster")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Reconcile OCI Repository
	if err := r.reconcileOCIRepository(ctx, kroVersion, ns); err != nil {
		_ = r.setStatus(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseFailed, fmt.Sprintf("OCI repository failed: %v", err))
		return ctrl.Result{}, err
	}

	// Reconcile HelmRelease #1: CRDs into workspace via kubeconfig
	kcpKubeconfigSecretName := fmt.Sprintf(kubeconfigSecretFmt, clusterName)
	if err := r.reconcileCRDHelmRelease(ctx, kroVersion, ns, kcpKubeconfigSecretName); err != nil {
		_ = r.setStatus(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseFailed, fmt.Sprintf("CRD HelmRelease failed: %v", err))
		return ctrl.Result{}, err
	}

	// Reconcile HelmRelease #2: kro controller into workload cluster, with kcp kubeconfig mounted
	if err := r.reconcileKroHelmRelease(ctx, kroVersion, ns, clusterName, workloadAccess.SecretRef); err != nil {
		_ = r.setStatus(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseFailed, fmt.Sprintf("Kro HelmRelease failed: %v", err))
		return ctrl.Result{}, err
	}

	// Copy kcp kubeconfig to workload cluster's kro-system namespace
	if err := r.replicateKCPKubeconfigToWorkload(ctx, ns, clusterName, workloadAccess.SecretRef); err != nil {
		_ = r.setStatus(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseFailed, fmt.Sprintf("Kubeconfig replication failed: %v", err))
		return ctrl.Result{}, err
	}

	// All done — only update status if not already Ready with correct version
	desiredMsg := fmt.Sprintf("kro %s installed", requestedVersion)
	currentPhase, _, _ := unstructured.NestedString(kvr.Object, "status", "phase")
	currentMsg, _, _ := unstructured.NestedString(kvr.Object, "status", "message")
	if currentPhase != string(apiv1alpha1.KroVersionRequestPhaseReady) || currentMsg != desiredMsg {
		_ = r.setStatusWithVersions(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseReady, desiredMsg, r.availableVersions())
	}

	l.Info("reconcile complete", "version", requestedVersion, "namespace", ns)
	// Requeue before token expires (at 80% of lifetime)
	requeueAfter := time.Until(tokenExpiry) / 5 // requeue at ~80% mark
	if requeueAfter < time.Minute {
		requeueAfter = time.Minute
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// isOldestInWorkspace checks if this KroVersionRequest is the oldest in the workspace.
func (r *KCPReconciler) isOldestInWorkspace(ctx context.Context, cl client.Client, current *unstructured.Unstructured) (bool, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   apiv1alpha1.GroupVersion.Group,
		Version: apiv1alpha1.GroupVersion.Version,
		Kind:    "KroVersionRequestList",
	})
	if err := cl.List(ctx, list); err != nil {
		return false, fmt.Errorf("listing KroVersionRequests: %w", err)
	}

	currentTime := current.GetCreationTimestamp().Time
	for _, item := range list.Items {
		if item.GetName() == current.GetName() {
			continue
		}
		if item.GetCreationTimestamp().Time.Before(currentTime) {
			return false, nil
		}
	}
	return true, nil
}

func (r *KCPReconciler) clusterClient(ctx context.Context, name multicluster.ClusterName) (client.Client, error) {
	cl, err := r.Manager.GetCluster(ctx, name)
	if err != nil {
		return nil, err
	}
	return cl.GetClient(), nil
}

func (r *KCPReconciler) handleDeletion(ctx context.Context, clusterName string, clusterClient client.Client) (ctrl.Result, error) {
	l := logf.FromContext(ctx)
	ns := tenantNamespace(clusterName)

	// ponytail: kro CRDs are left in the workspace intentionally — no RGD check needed.
	// Delete platform resources (HelmReleases + OCI repo)
	objects := []client.Object{
		&sourcev1.OCIRepository{ObjectMeta: metav1.ObjectMeta{Name: ociRepositoryName, Namespace: ns}},
		&helmv2.HelmRelease{ObjectMeta: metav1.ObjectMeta{Name: helmReleaseName, Namespace: ns}},
		&helmv2.HelmRelease{ObjectMeta: metav1.ObjectMeta{Name: crdHelmReleaseName, Namespace: ns}},
	}

	allGone := true
	for _, obj := range objects {
		if err := r.PlatformClient.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, fmt.Errorf("deleting %s: %w", obj.GetName(), err)
		}
		if err := r.PlatformClient.Get(ctx, client.ObjectKeyFromObject(obj), obj); !apierrors.IsNotFound(err) {
			allGone = false
		}
	}

	if !allGone {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Clean up workspace SA/RBAC/token (best effort)
	r.cleanupWorkspaceResources(ctx, clusterClient)

	// Delete platform namespace (contains kubeconfig secret + any leftover resources)
	platformNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}
	if err := r.PlatformClient.Delete(ctx, platformNS); client.IgnoreNotFound(err) != nil {
		l.Error(err, "failed to delete platform namespace", "namespace", ns)
	}

	l.Info("cleanup complete", "namespace", ns)
	return ctrl.Result{}, nil
}

func (r *KCPReconciler) cleanupWorkspaceResources(ctx context.Context, cl client.Client) {
	// Best-effort cleanup of SA, ClusterRole, ClusterRoleBinding
	_ = cl.Delete(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: serviceAccountName, Namespace: serviceAccountNamespace}})

	crb := &unstructured.Unstructured{}
	crb.SetGroupVersionKind(schema.GroupVersionKind{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRoleBinding"})
	crb.SetName(clusterRoleBindingName)
	_ = cl.Delete(ctx, crb)

	cr := &unstructured.Unstructured{}
	cr.SetGroupVersionKind(schema.GroupVersionKind{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRole"})
	cr.SetName(clusterRoleName)
	_ = cl.Delete(ctx, cr)
}

const tokenExpiryAnnotation = "service-provider-kro/token-expiry"

func (r *KCPReconciler) ensureKubeconfigSecret(ctx context.Context, namespace, clusterName string, kubeconfigData []byte, expiry time.Time) error {
	secretName := fmt.Sprintf(kubeconfigSecretFmt, clusterName)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace}}
	_, err := ctrl.CreateOrUpdate(ctx, r.PlatformClient, secret, func() error {
		secret.Labels = map[string]string{managedByLabel: managedByLabelValue}
		if secret.Annotations == nil {
			secret.Annotations = map[string]string{}
		}
		secret.Annotations[tokenExpiryAnnotation] = expiry.Format(time.RFC3339)
		secret.Type = corev1.SecretTypeOpaque
		secret.Data = map[string][]byte{
			kubeconfigKey: kubeconfigData,
		}
		return nil
	})
	return err
}

// readExistingToken reads the token and expiry from the existing kubeconfig secret.
// Returns empty string if no valid secret/token exists.
func (r *KCPReconciler) readExistingToken(ctx context.Context, namespace, clusterName string) (string, time.Time) {
	secretName := fmt.Sprintf(kubeconfigSecretFmt, clusterName)
	secret := &corev1.Secret{}
	if err := r.PlatformClient.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, secret); err != nil {
		return "", time.Time{}
	}
	expiryStr, ok := secret.Annotations[tokenExpiryAnnotation]
	if !ok {
		return "", time.Time{}
	}
	expiry, err := time.Parse(time.RFC3339, expiryStr)
	if err != nil {
		return "", time.Time{}
	}
	// We have an expiry, and there's kubeconfig data — return non-empty token sentinel
	if len(secret.Data[kubeconfigKey]) == 0 {
		return "", time.Time{}
	}
	return "valid", expiry
}

func (r *KCPReconciler) reconcileOCIRepository(ctx context.Context, version apiv1alpha1.KroVersion, namespace string) error {
	var secretRef *meta.LocalObjectReference
	if version.ChartPullSecret != "" {
		if err := r.replicateChartPullSecret(ctx, version.ChartPullSecret, namespace); err != nil {
			return err
		}
		secretRef = &meta.LocalObjectReference{Name: version.ChartPullSecret}
	}

	ociRepo := &sourcev1.OCIRepository{ObjectMeta: metav1.ObjectMeta{Name: ociRepositoryName, Namespace: namespace}}
	_, err := ctrl.CreateOrUpdate(ctx, r.PlatformClient, ociRepo, func() error {
		ociRepo.Spec = sourcev1.OCIRepositorySpec{
			Interval:  metav1.Duration{Duration: time.Minute},
			URL:       version.GetChartURL(),
			SecretRef: secretRef,
			Reference: &sourcev1.OCIRepositoryRef{Tag: version.ChartVersion},
		}
		return nil
	})
	return err
}

// reconcileCRDHelmRelease creates HelmRelease #1: installs kro CRDs into the kcp workspace.
func (r *KCPReconciler) reconcileCRDHelmRelease(ctx context.Context, version apiv1alpha1.KroVersion, namespace, kubeconfigSecretName string) error {
	helmRel := &helmv2.HelmRelease{ObjectMeta: metav1.ObjectMeta{Name: crdHelmReleaseName, Namespace: namespace}}
	_, err := ctrl.CreateOrUpdate(ctx, r.PlatformClient, helmRel, func() error {
		helmRel.Spec = helmv2.HelmReleaseSpec{
			ReleaseName:      "kro-crds",
			Interval:         metav1.Duration{Duration: time.Minute},
			TargetNamespace:  kroSystemNamespace,
			StorageNamespace: kroSystemNamespace,
			Install: &helmv2.Install{
				CRDs:            helmv2.CreateReplace,
				CreateNamespace: true,
				Remediation:     &helmv2.InstallRemediation{Retries: 3},
			},
			Upgrade: &helmv2.Upgrade{
				CRDs:          helmv2.CreateReplace,
				CleanupOnFail: true,
				Remediation: &helmv2.UpgradeRemediation{
					Retries:  3,
					Strategy: new(helmv2.RollbackRemediationStrategy),
				},
			},
			ChartRef: &helmv2.CrossNamespaceSourceReference{
				Kind:      "OCIRepository",
				Name:      ociRepositoryName,
				Namespace: namespace,
			},
			KubeConfig: &meta.KubeConfigReference{
				SecretRef: &meta.SecretKeyReference{
					Name: kubeconfigSecretName,
					Key:  kubeconfigKey,
				},
			},
			PostRenderers: crdOnlyPostRenderers(),
		}
		return nil
	})
	return err
}

// reconcileKroHelmRelease creates HelmRelease #2: installs kro controller into the workload cluster,
// with the kcp workspace kubeconfig mounted.
func (r *KCPReconciler) reconcileKroHelmRelease(ctx context.Context, version apiv1alpha1.KroVersion, namespace, clusterName string, workloadSecretRef *client.ObjectKey) error {
	helmRel := &helmv2.HelmRelease{ObjectMeta: metav1.ObjectMeta{Name: helmReleaseName, Namespace: namespace}}
	_, err := ctrl.CreateOrUpdate(ctx, r.PlatformClient, helmRel, func() error {
		helmRel.Spec = helmv2.HelmReleaseSpec{
			ReleaseName:      defaultReleaseName,
			Interval:         metav1.Duration{Duration: time.Minute},
			TargetNamespace:  kroSystemNamespace,
			StorageNamespace: kroSystemNamespace,
			Install: &helmv2.Install{
				CRDs:            helmv2.Skip,
				CreateNamespace: true,
				Remediation:     &helmv2.InstallRemediation{Retries: 3},
			},
			Upgrade: &helmv2.Upgrade{
				CRDs:          helmv2.Skip,
				CleanupOnFail: true,
				Remediation: &helmv2.UpgradeRemediation{
					Retries:  3,
					Strategy: new(helmv2.RollbackRemediationStrategy),
				},
			},
			ChartRef: &helmv2.CrossNamespaceSourceReference{
				Kind:      "OCIRepository",
				Name:      ociRepositoryName,
				Namespace: namespace,
			},
			Values: version.HelmValues,
			KubeConfig: &meta.KubeConfigReference{
				SecretRef: &meta.SecretKeyReference{
					Name: workloadSecretRef.Name,
					Key:  kubeconfigKey,
				},
			},
			PostRenderers: kcpKubeconfigPostRenderers(clusterName),
		}
		return nil
	})
	return err
}

// replicateKCPKubeconfigToWorkload copies the kcp kubeconfig secret from the platform
// cluster into kro-system on the workload cluster so the kro deployment can mount it.
// It builds a client to the workload cluster using the kubeconfig from workloadSecretRef.
func (r *KCPReconciler) replicateKCPKubeconfigToWorkload(ctx context.Context, namespace, clusterName string, workloadSecretRef *client.ObjectKey) error {
	// Read the kcp kubeconfig data from platform
	kcpSecretName := fmt.Sprintf(kubeconfigSecretFmt, clusterName)
	kcpSecret := &corev1.Secret{}
	if err := r.PlatformClient.Get(ctx, client.ObjectKey{Name: kcpSecretName, Namespace: namespace}, kcpSecret); err != nil {
		return fmt.Errorf("getting kcp kubeconfig secret: %w", err)
	}
	kubeconfigData := kcpSecret.Data[kubeconfigKey]
	if len(kubeconfigData) == 0 {
		return fmt.Errorf("kcp kubeconfig secret %s/%s has empty %q key", namespace, kcpSecretName, kubeconfigKey)
	}

	// Build a client to the workload cluster
	workloadClient, err := r.workloadClientFromSecret(ctx, workloadSecretRef)
	if err != nil {
		return fmt.Errorf("building workload client: %w", err)
	}

	// Ensure kro-system namespace exists on the workload cluster
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: kroSystemNamespace}}
	if _, err := ctrl.CreateOrUpdate(ctx, workloadClient, ns, func() error { return nil }); err != nil {
		return fmt.Errorf("ensuring namespace %s on workload cluster: %w", kroSystemNamespace, err)
	}

	// Create/update the kcp kubeconfig secret on the workload cluster
	target := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:      kcpSecretName,
		Namespace: kroSystemNamespace,
	}}
	if _, err := ctrl.CreateOrUpdate(ctx, workloadClient, target, func() error {
		target.Labels = map[string]string{managedByLabel: managedByLabelValue}
		target.Type = corev1.SecretTypeOpaque
		target.Data = map[string][]byte{
			kubeconfigKey: kubeconfigData,
		}
		return nil
	}); err != nil {
		return fmt.Errorf("creating kcp kubeconfig secret on workload cluster: %w", err)
	}

	return nil
}

func (r *KCPReconciler) replicateChartPullSecret(ctx context.Context, secretName, targetNamespace string) error {
	source := &corev1.Secret{}
	if err := r.PlatformClient.Get(ctx, client.ObjectKey{Name: secretName, Namespace: r.PodNamespace}, source); err != nil {
		return fmt.Errorf("getting chart pull secret %q: %w", secretName, err)
	}
	target := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: targetNamespace}}
	_, err := ctrl.CreateOrUpdate(ctx, r.PlatformClient, target, func() error {
		target.Data = source.Data
		target.Type = source.Type
		return nil
	})
	return err
}

// workloadClientFromSecret builds a controller-runtime client to the workload cluster
// using the kubeconfig stored in the referenced platform secret.
func (r *KCPReconciler) workloadClientFromSecret(ctx context.Context, secretRef *client.ObjectKey) (client.Client, error) {
	secret := &corev1.Secret{}
	if err := r.PlatformClient.Get(ctx, *secretRef, secret); err != nil {
		return nil, fmt.Errorf("getting workload kubeconfig secret %s: %w", secretRef, err)
	}

	kubeconfigData, ok := secret.Data[kubeconfigKey]
	if !ok {
		return nil, fmt.Errorf("workload kubeconfig secret %s missing %q key", secretRef, kubeconfigKey)
	}

	restCfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfigData)
	if err != nil {
		return nil, fmt.Errorf("building REST config from workload kubeconfig: %w", err)
	}

	// ponytail: plain client.New, no caching needed for occasional secret writes
	cl, err := client.New(restCfg, client.Options{Scheme: kcpScheme})
	if err != nil {
		return nil, fmt.Errorf("creating workload client: %w", err)
	}
	return cl, nil
}


func (r *KCPReconciler) setStatus(ctx context.Context, cl client.Client, obj *unstructured.Unstructured, phase apiv1alpha1.KroVersionRequestPhase, message string) error {
	return r.setStatusWithVersions(ctx, cl, obj, phase, message, nil)
}

func (r *KCPReconciler) setStatusWithVersions(ctx context.Context, cl client.Client, obj *unstructured.Unstructured, phase apiv1alpha1.KroVersionRequestPhase, message string, versions []string) error {
	status := map[string]interface{}{
		"phase":   string(phase),
		"message": message,
	}
	if versions != nil {
		versionInterfaces := make([]interface{}, len(versions))
		for i, v := range versions {
			versionInterfaces[i] = v
		}
		status["availableVersions"] = versionInterfaces
	}
	if phase == apiv1alpha1.KroVersionRequestPhaseReady {
		spec, _, _ := unstructured.NestedMap(obj.Object, "spec")
		if v, ok := spec["version"].(string); ok {
			status["installedVersion"] = v
		}
	}

	if err := unstructured.SetNestedMap(obj.Object, status, "status"); err != nil {
		return err
	}
	return cl.Status().Update(ctx, obj)
}

func (r *KCPReconciler) availableVersions() []string {
	versions := make([]string, 0, len(r.ProviderConfig.Spec.Versions))
	for _, v := range r.ProviderConfig.Spec.Versions {
		versions = append(versions, v.Version)
	}
	return versions
}

func (r *KCPReconciler) workspaceServerURL(clusterName string) string {
	// Extract scheme+host from the kcp config, stripping any workspace path,
	// then use the logical cluster path directly.
	host := r.KCPServerURL
	if u, err := url.Parse(host); err == nil {
		host = u.Scheme + "://" + u.Host
	}
	host = rewriteLoopbackForDocker(host)
	return fmt.Sprintf("%s/clusters/%s", host, clusterName)
}

// rewriteLoopbackForDocker rewrites 127.0.0.1 / localhost to host.docker.internal
// so that kubeconfigs stored in secrets work from inside Docker containers.
func rewriteLoopbackForDocker(serverURL string) string {
	for _, loopback := range []string{"://127.0.0.1", "://localhost"} {
		if idx := strings.Index(serverURL, loopback); idx >= 0 {
			return serverURL[:idx] + "://host.docker.internal" + serverURL[idx+len(loopback):]
		}
	}
	return serverURL
}

// kcpKubeconfigPostRenderers patches the kro deployment to mount and use
// the kcp workspace kubeconfig.
func kcpKubeconfigPostRenderers(clusterName string) []helmv2.PostRenderer {
	secretName := fmt.Sprintf(kubeconfigSecretFmt, clusterName)
	kubeconfigFilePath := kubeconfigMountPath + "/" + kubeconfigKey

	patch := fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: kro
spec:
  template:
    spec:
      containers:
        - name: kro
          env:
            - name: KUBECONFIG
              value: %s
          volumeMounts:
            - name: kcp-kubeconfig
              mountPath: %s
              readOnly: true
      volumes:
        - name: kcp-kubeconfig
          secret:
            secretName: %s`, kubeconfigFilePath, kubeconfigMountPath, secretName)

	return []helmv2.PostRenderer{
		{
			Kustomize: &helmv2.Kustomize{
				Patches: []kustomize.Patch{
					{
						Patch: patch,
						Target: &kustomize.Selector{
							Group:   "apps",
							Version: "v1",
							Kind:    "Deployment",
						},
					},
				},
			},
		},
	}
}

// crdOnlyPostRenderers strips all non-CRD resources from the chart.
func crdOnlyPostRenderers() []helmv2.PostRenderer {
	targets := []kustomize.Patch{
		{
			Patch: "$patch: delete\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: kro\n",
			Target: &kustomize.Selector{Group: "apps", Version: "v1", Kind: "Deployment"},
		},
		{
			Patch: "$patch: delete\napiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: kro\n",
			Target: &kustomize.Selector{Version: "v1", Kind: "ServiceAccount"},
		},
		{
			Patch: "$patch: delete\napiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata:\n  name: kro\n",
			Target: &kustomize.Selector{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRole"},
		},
		{
			Patch: "$patch: delete\napiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRoleBinding\nmetadata:\n  name: kro\n",
			Target: &kustomize.Selector{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRoleBinding"},
		},
		{
			Patch: "$patch: delete\napiVersion: v1\nkind: Service\nmetadata:\n  name: kro\n",
			Target: &kustomize.Selector{Version: "v1", Kind: "Service"},
		},
	}
	return []helmv2.PostRenderer{{Kustomize: &helmv2.Kustomize{Patches: targets}}}
}


// tenantNamespace derives a stable namespace name on the platform cluster.
func tenantNamespace(clusterName string) string {
	return fmt.Sprintf("kcp-%s", clusterName)
}

// ensurePlatformNamespace creates the namespace on the platform cluster if needed.
func ensurePlatformNamespace(ctx context.Context, cl client.Client, name string) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if _, err := ctrl.CreateOrUpdate(ctx, cl, ns, func() error {
		if ns.Labels == nil {
			ns.Labels = map[string]string{}
		}
		ns.Labels[managedByLabel] = managedByLabelValue
		return nil
	}); err != nil {
		return fmt.Errorf("ensuring namespace %s: %w", name, err)
	}
	return nil
}
