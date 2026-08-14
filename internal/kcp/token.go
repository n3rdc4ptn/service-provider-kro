package kcp

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"time"

	"github.com/kcp-dev/logicalcluster/v3"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// serviceAccountName is the SA created in each bound workspace for kro.
	serviceAccountName = "kro-service-provider"
	// serviceAccountNamespace is the namespace in the workspace where the SA lives.
	// "default" always exists in kcp workspaces — no need to create it.
	serviceAccountNamespace = "default"
	// clusterRoleName is the ClusterRole granting cluster-admin to the SA.
	clusterRoleName = "kro-service-provider-admin"
	// clusterRoleBindingName is the CRB binding the role to the SA.
	clusterRoleBindingName = "kro-service-provider-admin"
	// tokenLifetime for TokenRequest API.
	tokenLifetime = 24 * time.Hour
	// tokenRefreshThreshold is the fraction of lifetime at which we refresh.
	tokenRefreshThreshold = 0.8

	// managedByLabel marks resources we own in the workspace.
	managedByLabel      = "app.kubernetes.io/managed-by"
	managedByLabelValue = "service-provider-kro"
)

// WorkspaceTokenResult holds the outcome of workspace token provisioning.
type WorkspaceTokenResult struct {
	// Token is the raw bearer token for the workspace SA.
	Token string
	// ExpirationTimestamp is when the token expires.
	ExpirationTimestamp time.Time
}

// EnsureWorkspaceToken ensures a ServiceAccount, ClusterRole, ClusterRoleBinding exist
// in the consumer workspace (via virtual workspace client), then requests a token
// via the TokenRequest API (via direct workspace client).
func EnsureWorkspaceToken(ctx context.Context, workspaceClient client.Client, kcpConfig *rest.Config) (*WorkspaceTokenResult, error) {
	l := logf.FromContext(ctx)

	// Ensure ServiceAccount
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name:      serviceAccountName,
		Namespace: serviceAccountNamespace,
	}}
	if _, err := ctrl.CreateOrUpdate(ctx, workspaceClient, sa, func() error {
		if sa.Labels == nil {
			sa.Labels = map[string]string{}
		}
		sa.Labels[managedByLabel] = managedByLabelValue
		return nil
	}); err != nil {
		return nil, fmt.Errorf("ensuring ServiceAccount: %w", err)
	}

	// Ensure ClusterRole (cluster-admin equivalent)
	cr := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName}}
	if _, err := ctrl.CreateOrUpdate(ctx, workspaceClient, cr, func() error {
		cr.Labels = map[string]string{managedByLabel: managedByLabelValue}
		cr.Rules = []rbacv1.PolicyRule{
			{
				APIGroups: []string{"*"},
				Resources: []string{"*"},
				Verbs:     []string{"*"},
			},
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("ensuring ClusterRole: %w", err)
	}

	// Ensure ClusterRoleBinding
	crb := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleBindingName}}
	if _, err := ctrl.CreateOrUpdate(ctx, workspaceClient, crb, func() error {
		crb.Labels = map[string]string{managedByLabel: managedByLabelValue}
		crb.RoleRef = rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     clusterRoleName,
		}
		crb.Subjects = []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      serviceAccountName,
				Namespace: serviceAccountNamespace,
			},
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("ensuring ClusterRoleBinding: %w", err)
	}

	// Re-read SA to get the logical cluster annotation
	if err := workspaceClient.Get(ctx, client.ObjectKeyFromObject(sa), sa); err != nil {
		return nil, fmt.Errorf("re-reading ServiceAccount: %w", err)
	}

	// Request token via TokenRequest API using direct workspace client
	tokenResult, err := tokenFromServiceAccount(ctx, kcpConfig, sa)
	if err != nil {
		return nil, fmt.Errorf("requesting token: %w", err)
	}

	l.Info("token issued", "expiration", tokenResult.ExpirationTimestamp)
	return tokenResult, nil
}

// tokenFromServiceAccount creates an ephemeral token via the TokenRequest API,
// using a direct client to the workspace (derived from logicalcluster annotation on the SA).
func tokenFromServiceAccount(ctx context.Context, kcpConfig *rest.Config, sa *corev1.ServiceAccount) (*WorkspaceTokenResult, error) {
	clusterName := logicalcluster.From(sa)
	if clusterName.Empty() {
		return nil, fmt.Errorf("service account %s/%s missing annotation %s", sa.Namespace, sa.Name, logicalcluster.AnnotationKey)
	}

	u, err := url.Parse(kcpConfig.Host)
	if err != nil {
		return nil, fmt.Errorf("parsing kcp config host: %w", err)
	}

	wsConfig := rest.CopyConfig(kcpConfig)
	wsConfig.Host = (&url.URL{Scheme: u.Scheme, Host: u.Host}).String() + clusterName.Path().RequestPath()

	clientset, err := kubernetes.NewForConfig(wsConfig)
	if err != nil {
		return nil, fmt.Errorf("creating clientset for cluster %s: %w", clusterName, err)
	}

	expirationSeconds := int64(tokenLifetime.Seconds())
	tr := &authenticationv1.TokenRequest{
		Spec: authenticationv1.TokenRequestSpec{
			ExpirationSeconds: &expirationSeconds,
		},
	}

	result, err := clientset.CoreV1().ServiceAccounts(sa.Namespace).CreateToken(ctx, sa.Name, tr, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("creating token for SA %s/%s: %w", sa.Namespace, sa.Name, err)
	}

	return &WorkspaceTokenResult{
		Token:               result.Status.Token,
		ExpirationTimestamp: result.Status.ExpirationTimestamp.Time,
	}, nil
}

// IsTokenExpiring returns true if the token has passed the refresh threshold of its lifetime.
func IsTokenExpiring(expirationTime time.Time) bool {
	remaining := time.Until(expirationTime)
	// Refresh when less than 20% of lifetime remains
	return remaining < time.Duration(float64(tokenLifetime)*(1-tokenRefreshThreshold))
}

// BuildKubeconfig creates a kubeconfig YAML byte slice for the given server
// URL, token, and optional CA cert.
func BuildKubeconfig(serverURL, token string, caCert []byte) []byte {
	caData := ""
	insecure := "true"
	if len(caCert) > 0 {
		caData = fmt.Sprintf("    certificate-authority-data: %s", base64.StdEncoding.EncodeToString(caCert))
		insecure = "false"
	}

	// ponytail: raw string template, no yaml lib needed for this shape
	kubeconfig := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- cluster:
    server: %s
    insecure-skip-tls-verify: %s
%s
  name: kcp-workspace
contexts:
- context:
    cluster: kcp-workspace
    user: kro-service-provider
  name: default
current-context: default
users:
- name: kro-service-provider
  user:
    token: %s
`, serverURL, insecure, caData, token)

	return []byte(kubeconfig)
}
