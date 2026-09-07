package kcp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTenantNamespace(t *testing.T) {
	assert.Equal(t, "kcp-abc123", tenantNamespace("abc123"))
	assert.Equal(t, "kcp-root--consumer", tenantNamespace("root:consumer"))
	assert.Equal(t, "kcp-root--org--ws", tenantNamespace("root:org:ws"))
}

func TestWorkloadNamespace(t *testing.T) {
	assert.Equal(t, "kro-abc123", workloadNamespace("abc123"))
	assert.Equal(t, "kro-root--consumer", workloadNamespace("root:consumer"))
	assert.Equal(t, "kro-root--org--ws", workloadNamespace("root:org:ws"))
}

func TestIsTokenExpiring(t *testing.T) {
	// Expires in 20 hours - should NOT need refresh (still >20% remaining)
	assert.False(t, IsTokenExpiring(time.Now().Add(20*time.Hour)))

	// Expires in 4 hours - SHOULD need refresh (<20% of 24h = 4.8h)
	assert.True(t, IsTokenExpiring(time.Now().Add(4*time.Hour)))

	// Already expired - SHOULD need refresh
	assert.True(t, IsTokenExpiring(time.Now().Add(-1*time.Hour)))
}

func TestBuildKubeconfig(t *testing.T) {
	kc := BuildKubeconfig("https://kcp.example.com/clusters/test", "my-token", nil)
	got := string(kc)

	assert.Contains(t, got, "server: https://kcp.example.com/clusters/test")
	assert.Contains(t, got, "token: my-token")
	assert.Contains(t, got, "insecure-skip-tls-verify: true")
}

func TestBuildKubeconfigWithCA(t *testing.T) {
	ca := []byte("fake-ca-data")
	kc := BuildKubeconfig("https://kcp.example.com/clusters/test", "my-token", ca)
	got := string(kc)

	assert.Contains(t, got, "certificate-authority-data:")
	assert.Contains(t, got, "insecure-skip-tls-verify: false")
}

