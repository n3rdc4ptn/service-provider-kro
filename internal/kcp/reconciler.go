package kcp

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	apiv1alpha1 "github.com/openmcp-project/service-provider-kro/api/v1alpha1"
)

// Reconciler reconciles KroVersionRequest objects across consumer workspaces.
type Reconciler struct {
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
	// KCPConfig carries admin credentials for TokenRequest via the kcp front-proxy.
	// The virtual workspace client (per-cluster) is used for SA/RBAC, but subresources
	// like serviceaccounts/token are not proxied by the virtual workspace.
	KCPConfig *rest.Config
}

// Reconcile handles a KroVersionRequest event from a consumer workspace.
func (r *Reconciler) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
	clusterName := string(req.ClusterName)
	l := logf.FromContext(ctx).WithValues("cluster", clusterName, "name", req.Name)
	l.Info("reconciling KroVersionRequest")

	inst := &Installation{
		PlatformClient: r.PlatformClient,
		ClusterName:    clusterName,
		PodNamespace:   r.PodNamespace,
	}

	cl, err := r.clusterClient(ctx, req.ClusterName)
	if err != nil {
		// Workspace gone (APIBinding removed) — still clean up platform resources
		l.Info("workspace unreachable, cleaning up platform resources", "error", err)
		return r.handleInstallationDelete(ctx, inst, nil, nil)
	}

	// Read the KroVersionRequest from the workspace
	kvr := r.newKroVersionRequest()
	if err := cl.Get(ctx, client.ObjectKey{Name: req.Name}, kvr); err != nil {
		if client.IgnoreNotFound(err) == nil {
			l.Info("KroVersionRequest deleted, cleaning up")
			return r.handleInstallationDelete(ctx, inst, cl, kvr)
		}
		return ctrl.Result{}, fmt.Errorf("getting KroVersionRequest: %w", err)
	}

	if kvr.GetDeletionTimestamp() != nil {
		l.Info("KroVersionRequest being deleted, cleaning up")
		return r.handleInstallationDelete(ctx, inst, cl, kvr)
	}

	// Enforce singleton: only the oldest KroVersionRequest per workspace is active
	if isOldest, err := r.isOldestInWorkspace(ctx, cl, kvr); err != nil {
		return ctrl.Result{}, err
	} else if !isOldest {
		l.Info("rejecting duplicate KroVersionRequest, not the oldest")
		_ = r.setStatus(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseFailed,
			"Only one KroVersionRequest per workspace is allowed. This is not the oldest.")
		return ctrl.Result{}, nil
	}

	return r.reconcileInstall(ctx, cl, inst, kvr)
}

// reconcileInstall validates the spec version, resolves a token, runs Ensure,
// updates status, and returns the requeue result. It is split out of Reconcile
// to keep cyclomatic complexity within the linter threshold.
func (r *Reconciler) reconcileInstall(ctx context.Context, cl client.Client, inst *Installation, kvr *unstructured.Unstructured) (ctrl.Result, error) {
	l := logf.FromContext(ctx)

	// Extract and validate version
	spec, _, _ := unstructured.NestedMap(kvr.Object, "spec")
	requestedVersion, _ := spec["version"].(string)
	if requestedVersion == "" {
		_ = r.setStatus(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseFailed, "spec.version is required")
		return ctrl.Result{}, nil
	}

	kroVersion, err := r.ProviderConfig.ResolveVersion(requestedVersion)
	if err != nil {
		_ = r.setStatusWithVersions(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseFailed, err.Error(), r.availableVersions())
		return ctrl.Result{}, nil // user error, don't requeue
	}

	// Resolve workspace token
	kubeconfigData, tokenExpiry, err := r.resolveToken(ctx, cl, inst)
	if err != nil {
		_ = r.setStatus(ctx, cl, kvr, apiv1alpha1.KroVersionRequestPhaseFailed, fmt.Sprintf("Token provisioning failed: %v", err))
		return ctrl.Result{}, err
	}

	// Run the installation
	status := inst.Ensure(ctx, EnsureParams{
		Version:        kroVersion,
		KubeconfigData: kubeconfigData,
		TokenExpiry:    tokenExpiry,
	})

	// Update KroVersionRequest status
	_ = r.setStatusWithVersions(ctx, cl, kvr, status.Phase, status.Message, r.availableVersions())

	if status.Phase != apiv1alpha1.KroVersionRequestPhaseReady {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Requeue before token expires (at ~80% of lifetime)
	requeueAfter := time.Until(status.TokenExpiry) / 5
	if requeueAfter < time.Minute {
		requeueAfter = time.Minute
	}
	l.Info("reconcile complete", "version", requestedVersion, "requeueAfter", requeueAfter)
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// handleInstallationDelete runs Installation.Delete and updates status if possible.
func (r *Reconciler) handleInstallationDelete(ctx context.Context, inst *Installation, cl client.Client, kvr *unstructured.Unstructured) (ctrl.Result, error) {
	status := inst.Delete(ctx)

	// Best-effort workspace cleanup (SA/RBAC) — only if we still have access
	if cl != nil {
		r.cleanupWorkspaceResources(ctx, cl)
	}

	// Update status on the KroVersionRequest if we can
	if cl != nil && kvr != nil {
		_ = r.setStatus(ctx, cl, kvr, status.Phase, status.Message)
	}

	if status.Message != "Cleanup complete" {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

// resolveToken returns a valid kubeconfig and token expiry, refreshing if needed.
func (r *Reconciler) resolveToken(ctx context.Context, workspaceClient client.Client, inst *Installation) ([]byte, time.Time, error) {
	// Check for cached token
	if valid, expiry := inst.ReadExistingToken(ctx); valid && !IsTokenExpiring(expiry) {
		secret := &corev1.Secret{}
		if err := r.PlatformClient.Get(ctx, client.ObjectKey{
			Name:      inst.kubeconfigSecretName(),
			Namespace: inst.namespace(),
		}, secret); err == nil {
			if data := secret.Data[kubeconfigKey]; len(data) > 0 {
				return data, expiry, nil
			}
		}
	}

	// Need fresh token — SA/RBAC via virtual workspace client, TokenRequest via front-proxy.
	serverURL := r.workspaceServerURL(inst.ClusterName)
	tokenResult, err := EnsureWorkspaceToken(ctx, workspaceClient, r.KCPConfig)
	if err != nil {
		return nil, time.Time{}, err
	}

	kubeconfigData := BuildKubeconfig(serverURL, tokenResult.Token, nil)
	return kubeconfigData, tokenResult.ExpirationTimestamp, nil
}

// --- helpers ---

func (r *Reconciler) newKroVersionRequest() *unstructured.Unstructured {
	kvr := &unstructured.Unstructured{}
	kvr.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   apiv1alpha1.GroupVersion.Group,
		Version: apiv1alpha1.GroupVersion.Version,
		Kind:    "KroVersionRequest",
	})
	return kvr
}

func (r *Reconciler) clusterClient(ctx context.Context, name multicluster.ClusterName) (client.Client, error) {
	cl, err := r.Manager.GetCluster(ctx, name)
	if err != nil {
		return nil, err
	}
	return cl.GetClient(), nil
}

func (r *Reconciler) isOldestInWorkspace(ctx context.Context, cl client.Client, current *unstructured.Unstructured) (bool, error) {
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

func (r *Reconciler) cleanupWorkspaceResources(ctx context.Context, cl client.Client) {
	// Best-effort cleanup of SA, ClusterRole, ClusterRoleBinding in the workspace
	sa := &unstructured.Unstructured{}
	sa.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "ServiceAccount"})
	sa.SetName(serviceAccountName)
	sa.SetNamespace(serviceAccountNamespace)
	_ = cl.Delete(ctx, sa)

	crb := &unstructured.Unstructured{}
	crb.SetGroupVersionKind(schema.GroupVersionKind{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRoleBinding"})
	crb.SetName(clusterRoleBindingName)
	_ = cl.Delete(ctx, crb)

	cr := &unstructured.Unstructured{}
	cr.SetGroupVersionKind(schema.GroupVersionKind{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRole"})
	cr.SetName(clusterRoleName)
	_ = cl.Delete(ctx, cr)
}

func (r *Reconciler) setStatus(ctx context.Context, cl client.Client, obj *unstructured.Unstructured, phase apiv1alpha1.KroVersionRequestPhase, message string) error {
	return r.setStatusWithVersions(ctx, cl, obj, phase, message, nil)
}

func (r *Reconciler) setStatusWithVersions(ctx context.Context, cl client.Client, obj *unstructured.Unstructured, phase apiv1alpha1.KroVersionRequestPhase, message string, versions []string) error {
	status := map[string]any{
		"phase":   string(phase),
		"message": message,
	}
	if versions != nil {
		versionInterfaces := make([]any, len(versions))
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

func (r *Reconciler) availableVersions() []string {
	versions := make([]string, 0, len(r.ProviderConfig.Spec.Versions))
	for _, v := range r.ProviderConfig.Spec.Versions {
		versions = append(versions, v.Version)
	}
	return versions
}

func (r *Reconciler) workspaceServerURL(clusterName string) string {
	host := r.KCPServerURL
	// If an external URL is configured, use it — the controller may connect via
	// localhost/port-forward while workloads need a stable in-cluster address.
	if r.ProviderConfig.Spec.KCP != nil && r.ProviderConfig.Spec.KCP.ExternalURL != "" {
		host = r.ProviderConfig.Spec.KCP.ExternalURL
	}
	if u, err := url.Parse(host); err == nil {
		host = u.Scheme + "://" + u.Host
	}
	return fmt.Sprintf("%s/clusters/%s", host, clusterName)
}

// sanitizeClusterName replaces characters illegal in Kubernetes namespace names.
func sanitizeClusterName(clusterName string) string {
	return strings.ReplaceAll(clusterName, ":", "--")
}

// tenantNamespace derives a stable namespace name on the platform cluster.
func tenantNamespace(clusterName string) string {
	return fmt.Sprintf("kcp-%s", sanitizeClusterName(clusterName))
}

// workloadNamespace derives a per-tenant namespace on the workload cluster.
func workloadNamespace(clusterName string) string {
	return fmt.Sprintf("kro-%s", sanitizeClusterName(clusterName))
}

// ensurePlatformNamespace creates the namespace on the platform cluster if needed.
func ensurePlatformNamespace(ctx context.Context, cl client.Client, name string) error {
	ns := &corev1.Namespace{}
	ns.SetName(name)
	if err := cl.Get(ctx, client.ObjectKey{Name: name}, ns); err == nil {
		return nil
	}
	ns.SetLabels(map[string]string{managedByLabel: managedByLabelValue})
	return cl.Create(ctx, ns)
}
