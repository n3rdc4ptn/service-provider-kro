package kcp

import (
	"context"
	"fmt"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	"github.com/openmcp-project/openmcp-operator/api/common"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	workloadRequestSuffix = "--wl"
)

// WorkloadAccessResult holds the outcome of a workload cluster access request.
type WorkloadAccessResult struct {
	// Granted is true when the workload cluster is ready.
	Granted bool
	// SecretRef is the reference to the kubeconfig secret for the workload cluster.
	// Only set when Granted is true.
	SecretRef *client.ObjectKey
}

// EnsureWorkloadAccess creates or checks a ClusterRequest + AccessRequest for a
// workload cluster on the platform cluster. This is the kcp-mode equivalent of
// the workload half of ClusterAccessReconciler (MCP is not needed in kcp mode).
//
// namespace is the platform namespace for this tenant (from tenantNamespace()).
// name is used to derive stable request names.
func EnsureWorkloadAccess(ctx context.Context, platformClient client.Client, namespace, name string) (WorkloadAccessResult, error) {
	l := logf.FromContext(ctx)
	result := WorkloadAccessResult{}

	if err := ensurePlatformNamespace(ctx, platformClient, namespace); err != nil {
		return result, err
	}

	requestName := name + workloadRequestSuffix

	cr, err := ensureClusterReq(ctx, platformClient, requestName, namespace)
	if err != nil {
		return result, fmt.Errorf("ensuring ClusterRequest: %w", err)
	}

	if cr.Status.IsDenied() {
		return result, fmt.Errorf("workload ClusterRequest %s/%s denied", namespace, requestName)
	}
	if !cr.Status.IsGranted() {
		l.Info("workload ClusterRequest not yet granted", "name", requestName)
		return result, nil
	}

	ar, err := ensureAccessReq(ctx, platformClient, requestName, namespace)
	if err != nil {
		return result, fmt.Errorf("ensuring AccessRequest: %w", err)
	}

	if ar.Status.IsDenied() {
		return result, fmt.Errorf("workload AccessRequest %s/%s denied", namespace, requestName)
	}
	if !ar.Status.IsGranted() {
		l.Info("workload AccessRequest not yet granted", "name", requestName)
		return result, nil
	}

	if ar.Status.SecretRef == nil {
		return result, fmt.Errorf("workload AccessRequest %s/%s granted but no secretRef", namespace, requestName)
	}

	result.Granted = true
	result.SecretRef = &client.ObjectKey{
		Name:      ar.Status.SecretRef.Name,
		Namespace: namespace,
	}
	return result, nil
}

func ensureClusterReq(ctx context.Context, cl client.Client, name, namespace string) (*clustersv1alpha1.ClusterRequest, error) {
	cr := &clustersv1alpha1.ClusterRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}

	err := cl.Get(ctx, client.ObjectKeyFromObject(cr), cr)
	if err == nil {
		return cr, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	cr.Spec = clustersv1alpha1.ClusterRequestSpec{
		Purpose: clustersv1alpha1.PURPOSE_WORKLOAD,
	}
	cr.Labels = map[string]string{managedByLabel: managedByLabelValue}
	if err := cl.Create(ctx, cr); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return cr, cl.Get(ctx, client.ObjectKeyFromObject(cr), cr)
		}
		return nil, err
	}
	return cr, nil
}

func ensureAccessReq(ctx context.Context, cl client.Client, name, namespace string) (*clustersv1alpha1.AccessRequest, error) {
	ar := &clustersv1alpha1.AccessRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}

	err := cl.Get(ctx, client.ObjectKeyFromObject(ar), ar)
	if err == nil {
		return ar, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	ar.Spec = clustersv1alpha1.AccessRequestSpec{
		RequestRef: &common.ObjectReference{
			Name:      name,
			Namespace: namespace,
		},
		Token: &clustersv1alpha1.TokenConfig{
			Permissions: []clustersv1alpha1.PermissionsRequest{
				{
					Rules: []rbacv1.PolicyRule{
						{
							APIGroups: []string{"*"},
							Resources: []string{"*"},
							Verbs:     []string{"*"},
						},
					},
				},
			},
			RoleRefs: []common.RoleRef{
				{
					Name: "cluster-admin",
					Kind: "ClusterRole",
				},
			},
		},
	}
	ar.Labels = map[string]string{managedByLabel: managedByLabelValue}
	if err := cl.Create(ctx, ar); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return ar, cl.Get(ctx, client.ObjectKeyFromObject(ar), ar)
		}
		return nil, err
	}
	return ar, nil
}
