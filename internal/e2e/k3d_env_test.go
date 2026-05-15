package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	defaultNamespace = "olric-e2e"
	defaultK3dCluster = "dev"
)

type k3dEnv struct {
	clusterName string
	namespace   string
	kubectl     string
}

func requireK3dEnv(t *testing.T) *k3dEnv {
	t.Helper()

	clusterName := envOrDefault("E2E_K3D_CLUSTER", defaultK3dCluster)
	namespace := envOrDefault("E2E_NAMESPACE", defaultNamespace)
	kubectl := envOrDefault("KUBECTL", "kubectl")

	if _, err := exec.LookPath(kubectl); err != nil {
		t.Skipf("kubectl not found: %v", err)
	}
	if _, err := exec.LookPath("k3d"); err != nil {
		t.Skipf("k3d not found: %v", err)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker not found: %v", err)
	}

	env := &k3dEnv{
		clusterName: clusterName,
		namespace:   namespace,
		kubectl:     kubectl,
	}
	if err := env.verifyCluster(); err != nil {
		t.Skipf("k3d environment not ready: %v", err)
	}
	return env
}

func (e *k3dEnv) verifyCluster() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "k3d", "cluster", "list")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("list clusters: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	if !strings.Contains(string(out), e.clusterName) {
		return fmt.Errorf("cluster %q not found in k3d output", e.clusterName)
	}
	return nil
}

func (e *k3dEnv) kubectlCmd(ctx context.Context, args ...string) *exec.Cmd {
	argv := append([]string{"--context", fmt.Sprintf("k3d-%s", e.clusterName)}, args...)
	return exec.CommandContext(ctx, e.kubectl, argv...)
}

func (e *k3dEnv) applyYAML(t *testing.T, yaml string) {
	t.Helper()
	e.applyYAMLInNamespace(t, e.namespace, yaml)
}

func (e *k3dEnv) applyYAMLInNamespace(t *testing.T, namespace, yaml string) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	args := []string{"apply"}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	args = append(args, "-f", path)
	cmd := e.kubectlCmd(ctx, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("apply manifest: %v (%s)", err, strings.TrimSpace(string(out)))
	}
}

func (e *k3dEnv) repoRoot(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working dir: %v", err)
	}
	root := filepath.Clean(filepath.Join(wd, "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	return root
}

func (e *k3dEnv) repoPath(t *testing.T, parts ...string) string {
	t.Helper()
	return filepath.Join(append([]string{e.repoRoot(t)}, parts...)...)
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
