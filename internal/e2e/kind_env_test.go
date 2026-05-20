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
	defaultNamespace   = "olric-e2e"
	defaultKindCluster = "olric-e2e"
)

type kindEnv struct {
	clusterName string
	kubeContext string
	namespace   string
	kubectl     string
}

func requireKindEnv(t *testing.T) *kindEnv {
	t.Helper()

	clusterName := envOrDefault("E2E_KIND_CLUSTER", defaultKindCluster)
	namespace := envOrDefault("E2E_NAMESPACE", defaultNamespace)
	kubectl := envOrDefault("KUBECTL", "kubectl")
	kubeContext := envOrDefault("E2E_KUBE_CONTEXT", "kind-"+clusterName)

	if _, err := exec.LookPath(kubectl); err != nil {
		t.Skipf("kubectl not found: %v", err)
	}
	if _, err := exec.LookPath("kind"); err != nil {
		t.Skipf("kind not found: %v", err)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker not found: %v", err)
	}

	env := &kindEnv{
		clusterName: clusterName,
		kubeContext: kubeContext,
		namespace:   namespace,
		kubectl:     kubectl,
	}
	if err := env.verifyCluster(); err != nil {
		t.Skipf("kind environment not ready: %v", err)
	}
	return env
}

func (e *kindEnv) verifyCluster() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "kind", "get", "clusters")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("list clusters: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(line) == e.clusterName {
			return nil
		}
	}
	return fmt.Errorf("cluster %q not found in kind output", e.clusterName)
}

func (e *kindEnv) kubectlCmd(ctx context.Context, args ...string) *exec.Cmd {
	argv := append([]string{"--context", e.kubeContext}, args...)
	return exec.CommandContext(ctx, e.kubectl, argv...)
}

func (e *kindEnv) applyYAML(t *testing.T, yaml string) {
	t.Helper()
	e.applyYAMLInNamespace(t, e.namespace, yaml)
}

func (e *kindEnv) applyYAMLInNamespace(t *testing.T, namespace, yaml string) {
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

func (e *kindEnv) repoRoot(t *testing.T) string {
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

func (e *kindEnv) repoPath(t *testing.T, parts ...string) string {
	t.Helper()
	return filepath.Join(append([]string{e.repoRoot(t)}, parts...)...)
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
