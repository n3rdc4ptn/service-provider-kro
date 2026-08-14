package kcp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/kustomize"
	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	apiv1alpha1 "github.com/openmcp-project/service-provider-kro/api/v1alpha1"
)

const (
	ociRepositoryName  = "kro-oci-repository"
	helmReleaseName    = "kro-helm-release"
	crdHelmReleaseName = "kro-crds-helm-release"
	defaultReleaseName = "kro"

	// kroSystemNamespace is used for the CRD-only HelmRelease targeting the kcp workspace.
	// Each workspace is isolated so a fixed name is safe.
	kroSystemNamespace = "kro-system"

	kubeconfigMountPath = "/etc/kro/kubeconfig"
	kubeconfigKey       = "kubeconfig"
	kubeconfigSecretFmt = "kcp-%s-kubeconfig"

	tokenExpiryAnnotation = "service-provider-kro/token-expiry"
)

// InstallationStatus is the result of an Ensure or Delete operation.
type InstallationStatus struct {
	// Phase for the KroVersionRequest status.
	Phase apiv1alpha1.KroVersionRequestPhase
	// Message for the KroVersionRequest status.
	Message string
	// TokenExpiry is when the workspace token expires (only set on successful Ensure).
	TokenExpiry time.Time
}

// Installation manages all platform-side resources for one tenant's kro deployment.
// It encapsulates: platform namespace, kubeconfig secret, ClusterRequest, AccessRequest,
// OCI repository, CRD HelmRelease, kro HelmRelease, and workload kubeconfig replication.
type Installation struct {
	PlatformClient client.Client
	ClusterName    string // raw kcp logical cluster name
	PodNamespace   string // controller's own namespace (for secret replication source)
}

// namespace on the platform cluster for this tenant.
func (i *Installation) namespace() string {
	return tenantNamespace(i.ClusterName)
}

// wlNamespace on the workload cluster for this tenant.
func (i *Installation) wlNamespace() string {
	return workloadNamespace(i.ClusterName)
}

// kubeconfigSecretName for the kcp workspace token.
func (i *Installation) kubeconfigSecretName() string {
	return fmt.Sprintf(kubeconfigSecretFmt, i.ClusterName)
}

// workloadRequestName for ClusterRequest/AccessRequest.
func (i *Installation) workloadRequestName() string {
	return i.ClusterName + workloadRequestSuffix
}

// Ensure creates or updates all platform resources for this tenant's kro installation.
func (i *Installation) Ensure(ctx context.Context, params EnsureParams) InstallationStatus {
	l := logf.FromContext(ctx)
	ns := i.namespace()

	// Check if any owned resource is still terminating from a previous deletion
	if terminating, msg := i.isTerminating(ctx); terminating {
		return InstallationStatus{
			Phase:   apiv1alpha1.KroVersionRequestPhaseProvisioning,
			Message: msg,
		}
	}

	// Ensure platform namespace
	if err := ensurePlatformNamespace(ctx, i.PlatformClient, ns); err != nil {
		return i.failed("Platform namespace failed: %v", err)
	}

	// Ensure kubeconfig secret with workspace token
	if err := i.ensureKubeconfigSecret(ctx, params.KubeconfigData, params.TokenExpiry); err != nil {
		return i.failed("Kubeconfig secret failed: %v", err)
	}

	// Ensure workload cluster access (ClusterRequest + AccessRequest)
	workloadAccess, err := EnsureWorkloadAccess(ctx, i.PlatformClient, ns, i.ClusterName)
	if err != nil {
		return i.failed("Workload access failed: %v", err)
	}
	if !workloadAccess.Granted {
		l.Info("workload cluster not yet ready")
		return InstallationStatus{
			Phase:   apiv1alpha1.KroVersionRequestPhaseProvisioning,
			Message: "Waiting for workload cluster",
		}
	}

	// Ensure OCI Repository
	if err := i.ensureOCIRepository(ctx, params.Version); err != nil {
		return i.failed("OCI repository failed: %v", err)
	}

	// Ensure CRD HelmRelease (→ kcp workspace)
	if err := i.ensureCRDHelmRelease(ctx, params.Version); err != nil {
		return i.failed("CRD HelmRelease failed: %v", err)
	}

	// Ensure kro HelmRelease (→ workload cluster)
	if err := i.ensureKroHelmRelease(ctx, params.Version, workloadAccess.SecretRef); err != nil {
		return i.failed("Kro HelmRelease failed: %v", err)
	}

	// Replicate kcp kubeconfig to workload cluster
	if err := i.replicateKubeconfigToWorkload(ctx, workloadAccess.SecretRef); err != nil {
		return i.failed("Kubeconfig replication failed: %v", err)
	}

	return InstallationStatus{
		Phase:       apiv1alpha1.KroVersionRequestPhaseReady,
		Message:     fmt.Sprintf("kro %s installed", params.Version.Version),
		TokenExpiry: params.TokenExpiry,
	}
}

// Delete removes all platform resources for this tenant. Returns status indicating
// whether cleanup is complete or still in progress (resources terminating).
func (i *Installation) Delete(ctx context.Context) InstallationStatus {
	l := logf.FromContext(ctx)
	ns := i.namespace()

	// Issue delete for all owned resources. Order doesn't matter — we check existence after.
	resources := i.ownedResources(ns)
	for _, obj := range resources {
		if err := i.PlatformClient.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
			l.Error(err, "failed to delete resource", "name", obj.GetName(), "kind", obj.GetObjectKind().GroupVersionKind().Kind)
		}
	}

	// Check if any still exist (including terminating)
	var remaining []string
	for _, obj := range resources {
		if err := i.PlatformClient.Get(ctx, client.ObjectKeyFromObject(obj), obj); err == nil {
			remaining = append(remaining, fmt.Sprintf("%s/%s", obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName()))
		}
	}

	if len(remaining) > 0 {
		l.Info("waiting for resources to terminate", "remaining", remaining)
		return InstallationStatus{
			Phase:   apiv1alpha1.KroVersionRequestPhaseTerminating,
			Message: fmt.Sprintf("Waiting for cleanup: %v", remaining),
		}
	}

	l.Info("cleanup complete", "namespace", ns)
	return InstallationStatus{
		Phase:   apiv1alpha1.KroVersionRequestPhaseTerminating,
		Message: "Cleanup complete",
	}
}

// EnsureParams holds the inputs for an Ensure call.
type EnsureParams struct {
	Version        apiv1alpha1.KroVersion
	KubeconfigData []byte
	TokenExpiry    time.Time
}

// --- private methods ---

func (i *Installation) failed(format string, args ...interface{}) InstallationStatus {
	return InstallationStatus{
		Phase:   apiv1alpha1.KroVersionRequestPhaseFailed,
		Message: fmt.Sprintf(format, args...),
	}
}

// ownedResources returns all platform-side resources this installation manages.
func (i *Installation) ownedResources(ns string) []client.Object {
	reqName := i.workloadRequestName()
	return []client.Object{
		&helmv2.HelmRelease{ObjectMeta: metav1.ObjectMeta{Name: helmReleaseName, Namespace: ns}},
		&helmv2.HelmRelease{ObjectMeta: metav1.ObjectMeta{Name: crdHelmReleaseName, Namespace: ns}},
		&sourcev1.OCIRepository{ObjectMeta: metav1.ObjectMeta{Name: ociRepositoryName, Namespace: ns}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: i.kubeconfigSecretName(), Namespace: ns}},
		&clustersv1alpha1.AccessRequest{ObjectMeta: metav1.ObjectMeta{Name: reqName, Namespace: ns}},
		&clustersv1alpha1.ClusterRequest{ObjectMeta: metav1.ObjectMeta{Name: reqName, Namespace: ns}},
	}
}

// isTerminating checks if any owned resource exists with a deletionTimestamp.
func (i *Installation) isTerminating(ctx context.Context) (bool, string) {
	ns := i.namespace()
	for _, obj := range i.ownedResources(ns) {
		if err := i.PlatformClient.Get(ctx, client.ObjectKeyFromObject(obj), obj); err == nil {
			if obj.GetDeletionTimestamp() != nil {
				kind := obj.GetObjectKind().GroupVersionKind().Kind
				// If GVK is empty (typed objects), use the struct type
				if kind == "" {
					kind = fmt.Sprintf("%T", obj)
				}
				return true, fmt.Sprintf("Waiting for %s/%s to terminate", kind, obj.GetName())
			}
		}
	}
	return false, ""
}

func (i *Installation) ensureKubeconfigSecret(ctx context.Context, kubeconfigData []byte, expiry time.Time) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: i.kubeconfigSecretName(), Namespace: i.namespace()}}
	_, err := ctrl.CreateOrUpdate(ctx, i.PlatformClient, secret, func() error {
		secret.Labels = map[string]string{managedByLabel: managedByLabelValue}
		if secret.Annotations == nil {
			secret.Annotations = map[string]string{}
		}
		secret.Annotations[tokenExpiryAnnotation] = expiry.Format(time.RFC3339)
		secret.Type = corev1.SecretTypeOpaque
		secret.Data = map[string][]byte{kubeconfigKey: kubeconfigData}
		return nil
	})
	return err
}

// ReadExistingToken reads the token expiry from the existing kubeconfig secret.
// Returns false if no valid cached token exists.
func (i *Installation) ReadExistingToken(ctx context.Context) (bool, time.Time) {
	secret := &corev1.Secret{}
	if err := i.PlatformClient.Get(ctx, client.ObjectKey{Name: i.kubeconfigSecretName(), Namespace: i.namespace()}, secret); err != nil {
		return false, time.Time{}
	}
	expiryStr, ok := secret.Annotations[tokenExpiryAnnotation]
	if !ok {
		return false, time.Time{}
	}
	expiry, err := time.Parse(time.RFC3339, expiryStr)
	if err != nil {
		return false, time.Time{}
	}
	if len(secret.Data[kubeconfigKey]) == 0 {
		return false, time.Time{}
	}
	return true, expiry
}

func (i *Installation) ensureOCIRepository(ctx context.Context, version apiv1alpha1.KroVersion) error {
	ns := i.namespace()
	var secretRef *meta.LocalObjectReference
	if version.ChartPullSecret != "" {
		if err := i.replicateChartPullSecret(ctx, version.ChartPullSecret); err != nil {
			return err
		}
		secretRef = &meta.LocalObjectReference{Name: version.ChartPullSecret}
	}

	ociRepo := &sourcev1.OCIRepository{ObjectMeta: metav1.ObjectMeta{Name: ociRepositoryName, Namespace: ns}}
	_, err := ctrl.CreateOrUpdate(ctx, i.PlatformClient, ociRepo, func() error {
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

func (i *Installation) ensureCRDHelmRelease(ctx context.Context, version apiv1alpha1.KroVersion) error {
	ns := i.namespace()
	helmRel := &helmv2.HelmRelease{ObjectMeta: metav1.ObjectMeta{Name: crdHelmReleaseName, Namespace: ns}}
	_, err := ctrl.CreateOrUpdate(ctx, i.PlatformClient, helmRel, func() error {
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
				Namespace: ns,
			},
			KubeConfig: &meta.KubeConfigReference{
				SecretRef: &meta.SecretKeyReference{
					Name: i.kubeconfigSecretName(),
					Key:  kubeconfigKey,
				},
			},
			PostRenderers: crdOnlyPostRenderers(),
		}
		return nil
	})
	return err
}

func (i *Installation) ensureKroHelmRelease(ctx context.Context, version apiv1alpha1.KroVersion, workloadSecretRef *client.ObjectKey) error {
	ns := i.namespace()
	helmRel := &helmv2.HelmRelease{ObjectMeta: metav1.ObjectMeta{Name: helmReleaseName, Namespace: ns}}
	_, err := ctrl.CreateOrUpdate(ctx, i.PlatformClient, helmRel, func() error {
		helmRel.Spec = helmv2.HelmReleaseSpec{
			ReleaseName:      defaultReleaseName,
			Interval:         metav1.Duration{Duration: time.Minute},
			TargetNamespace:  i.wlNamespace(),
			StorageNamespace: i.wlNamespace(),
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
				Namespace: ns,
			},
			Values: kcpHelmValues(version.HelmValues),
			KubeConfig: &meta.KubeConfigReference{
				SecretRef: &meta.SecretKeyReference{
					Name: workloadSecretRef.Name,
					Key:  kubeconfigKey,
				},
			},
			PostRenderers: kcpKubeconfigPostRenderers(i.ClusterName),
		}
		return nil
	})
	return err
}

func (i *Installation) replicateKubeconfigToWorkload(ctx context.Context, workloadSecretRef *client.ObjectKey) error {
	ns := i.namespace()
	kcpSecret := &corev1.Secret{}
	if err := i.PlatformClient.Get(ctx, client.ObjectKey{Name: i.kubeconfigSecretName(), Namespace: ns}, kcpSecret); err != nil {
		return fmt.Errorf("getting kcp kubeconfig secret: %w", err)
	}
	kubeconfigData := kcpSecret.Data[kubeconfigKey]
	if len(kubeconfigData) == 0 {
		return fmt.Errorf("kcp kubeconfig secret %s/%s has empty %q key", ns, i.kubeconfigSecretName(), kubeconfigKey)
	}

	workloadClient, err := i.workloadClientFromSecret(ctx, workloadSecretRef)
	if err != nil {
		return fmt.Errorf("building workload client: %w", err)
	}

	// Ensure per-tenant namespace on workload cluster
	wlNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: i.wlNamespace()}}
	if _, err := ctrl.CreateOrUpdate(ctx, workloadClient, wlNS, func() error {
		if wlNS.Labels == nil {
			wlNS.Labels = map[string]string{}
		}
		wlNS.Labels[managedByLabel] = managedByLabelValue
		return nil
	}); err != nil {
		return fmt.Errorf("ensuring namespace %s on workload cluster: %w", i.wlNamespace(), err)
	}

	target := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:      i.kubeconfigSecretName(),
		Namespace: i.wlNamespace(),
	}}
	if _, err := ctrl.CreateOrUpdate(ctx, workloadClient, target, func() error {
		target.Labels = map[string]string{managedByLabel: managedByLabelValue}
		target.Type = corev1.SecretTypeOpaque
		target.Data = map[string][]byte{kubeconfigKey: kubeconfigData}
		return nil
	}); err != nil {
		return fmt.Errorf("creating kcp kubeconfig secret on workload cluster: %w", err)
	}

	return nil
}

func (i *Installation) replicateChartPullSecret(ctx context.Context, secretName string) error {
	source := &corev1.Secret{}
	if err := i.PlatformClient.Get(ctx, client.ObjectKey{Name: secretName, Namespace: i.PodNamespace}, source); err != nil {
		return fmt.Errorf("getting chart pull secret %q: %w", secretName, err)
	}
	target := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: i.namespace()}}
	_, err := ctrl.CreateOrUpdate(ctx, i.PlatformClient, target, func() error {
		target.Data = source.Data
		target.Type = source.Type
		return nil
	})
	return err
}

// ponytail: plain client.New per call — no caching needed for occasional writes
func (i *Installation) workloadClientFromSecret(ctx context.Context, secretRef *client.ObjectKey) (client.Client, error) {
	secret := &corev1.Secret{}
	if err := i.PlatformClient.Get(ctx, *secretRef, secret); err != nil {
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
	cl, err := client.New(restCfg, client.Options{Scheme: kcpScheme})
	if err != nil {
		return nil, fmt.Errorf("creating workload client: %w", err)
	}
	return cl, nil
}

// --- Helm helpers ---

// kcpKubeconfigPostRenderers patches the kro deployment to mount the kcp workspace kubeconfig.
func kcpKubeconfigPostRenderers(clusterName string) []helmv2.PostRenderer {
	secretName := fmt.Sprintf(kubeconfigSecretFmt, clusterName)
	kubeconfigFilePath := kubeconfigMountPath + "/" + kubeconfigKey

	// ponytail: leader election namespace is set via helm values (config.leaderElectionNamespace)
	// since "default" always exists in kcp workspaces but the per-tenant workload namespace does not.
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
			Patch:  "$patch: delete\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: kro\n",
			Target: &kustomize.Selector{Group: "apps", Version: "v1", Kind: "Deployment"},
		},
		{
			Patch:  "$patch: delete\napiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: kro\n",
			Target: &kustomize.Selector{Version: "v1", Kind: "ServiceAccount"},
		},
		{
			Patch:  "$patch: delete\napiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata:\n  name: kro\n",
			Target: &kustomize.Selector{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRole"},
		},
		{
			Patch:  "$patch: delete\napiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRoleBinding\nmetadata:\n  name: kro\n",
			Target: &kustomize.Selector{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRoleBinding"},
		},
		{
			Patch:  "$patch: delete\napiVersion: v1\nkind: Service\nmetadata:\n  name: kro\n",
			Target: &kustomize.Selector{Version: "v1", Kind: "Service"},
		},
	}
	return []helmv2.PostRenderer{{Kustomize: &helmv2.Kustomize{Patches: targets}}}
}

// kcpHelmValues merges config.leaderElectionNamespace="default" into the user-provided
// helm values. Our override always wins.
func kcpHelmValues(userValues *apiextensionsv1.JSON) *apiextensionsv1.JSON {
	values := map[string]interface{}{}
	if userValues != nil && len(userValues.Raw) > 0 {
		_ = json.Unmarshal(userValues.Raw, &values)
	}
	override := map[string]interface{}{
		"config": map[string]interface{}{
			"leaderElectionNamespace": "default",
		},
	}
	mergeValues(values, override)
	raw, _ := json.Marshal(values)
	return &apiextensionsv1.JSON{Raw: raw}
}

// mergeValues deep-merges src into dst. src values win for non-map keys.
func mergeValues(dst, src map[string]interface{}) {
	for k, v := range src {
		if srcMap, ok := v.(map[string]interface{}); ok {
			if dstMap, ok := dst[k].(map[string]interface{}); ok {
				mergeValues(dstMap, srcMap)
				continue
			}
		}
		dst[k] = v
	}
}
