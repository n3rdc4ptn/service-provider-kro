package kcp

import (
	"context"
	"fmt"

	"github.com/kcp-dev/multicluster-provider/apiexport"
	apisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	apisv1alpha2 "github.com/kcp-dev/sdk/apis/apis/v1alpha2"
	corev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	tenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"
	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	apiv1alpha1 "github.com/openmcp-project/service-provider-kro/api/v1alpha1"
)

// kcpScheme is the scheme used by the kcp multicluster manager.
var kcpScheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(kcpScheme))
	utilruntime.Must(apisv1alpha1.AddToScheme(kcpScheme))
	utilruntime.Must(apisv1alpha2.AddToScheme(kcpScheme))
	utilruntime.Must(corev1alpha1.AddToScheme(kcpScheme))
	utilruntime.Must(tenancyv1alpha1.AddToScheme(kcpScheme))
	utilruntime.Must(apiv1alpha1.AddToScheme(kcpScheme))
	utilruntime.Must(helmv2.AddToScheme(kcpScheme))
	utilruntime.Must(sourcev1.AddToScheme(kcpScheme))

	// Ensure rbac and core types are registered for workspace operations
	_ = rbacv1.AddToScheme(kcpScheme)
	_ = corev1.AddToScheme(kcpScheme)
}

// Scheme returns the scheme containing kcp API types.
func Scheme() *runtime.Scheme {
	return kcpScheme
}

// SetupConfig holds everything needed to start the kcp-mode controller.
type SetupConfig struct {
	// PlatformClient talks to the platform cluster (for Flux resources).
	PlatformClient client.Client
	// PodNamespace is the controller's namespace on the platform cluster.
	PodNamespace string
	// ProviderConfig is the current provider configuration.
	ProviderConfig *apiv1alpha1.ProviderConfig
}

// RestConfig builds a rest.Config for the kcp API server from the
// kubeconfig secret referenced in the ProviderConfig.
func RestConfig(ctx context.Context, platformClient client.Client, kcpCfg *apiv1alpha1.KCPConfig) (*rest.Config, error) {
	secret := &corev1.Secret{}
	key := client.ObjectKey{
		Name:      kcpCfg.KubeconfigSecret.Name,
		Namespace: kcpCfg.KubeconfigSecret.Namespace,
	}
	if err := platformClient.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("getting kcp kubeconfig secret %s: %w", key, err)
	}

	kubeconfigData, ok := secret.Data["kubeconfig"]
	if !ok {
		return nil, fmt.Errorf("kcp kubeconfig secret %s missing 'kubeconfig' key", key)
	}

	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfigData)
	if err != nil {
		return nil, fmt.Errorf("building rest.Config from kcp kubeconfig: %w", err)
	}
	return cfg, nil
}

// SetupManager creates a multicluster-runtime manager wired to the kcp
// apiexport provider. It watches KroVersionRequest objects across consumer workspaces.
func SetupManager(_ context.Context, kcpCfg *rest.Config, cfg SetupConfig) (mcmanager.Manager, error) {
	pc := cfg.ProviderConfig
	if pc.Spec.KCP == nil {
		return nil, fmt.Errorf("ProviderConfig.Spec.KCP must be set for kcp mode")
	}

	provider, err := apiexport.New(kcpCfg, APIExportName, apiexport.Options{
		Scheme: kcpScheme,
	})
	if err != nil {
		return nil, fmt.Errorf("creating kcp apiexport provider: %w", err)
	}

	mgr, err := mcmanager.New(kcpCfg, provider, manager.Options{
		Scheme: kcpScheme,
	})
	if err != nil {
		return nil, fmt.Errorf("creating multicluster manager: %w", err)
	}

	reconciler := &Reconciler{
		Manager:        mgr,
		PlatformClient: cfg.PlatformClient,
		PodNamespace:   cfg.PodNamespace,
		ProviderConfig: cfg.ProviderConfig,
		KCPServerURL:   kcpCfg.Host,
		KCPConfig:      kcpCfg,
	}

	// Watch KroVersionRequest objects across all consumer workspaces.
	kroVersionRequestGVK := schema.GroupVersionKind{
		Group:   apiv1alpha1.GroupVersion.Group,
		Version: apiv1alpha1.GroupVersion.Version,
		Kind:    "KroVersionRequest",
	}
	watchObj := &unstructured.Unstructured{}
	watchObj.SetGroupVersionKind(kroVersionRequestGVK)

	if err := mcbuilder.ControllerManagedBy(mgr).
		Named("kcp-kroversion-controller").
		For(watchObj).
		Complete(mcreconcile.Func(reconciler.Reconcile)); err != nil {
		return nil, fmt.Errorf("building kcp controller: %w", err)
	}

	return mgr, nil
}
