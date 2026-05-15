//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestK3dStackReconcilesThroughWatchdog(t *testing.T) {
	env := requireK3dEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	stackName := envOrDefault("E2E_STACK_NAME", "demo")
	nodeImage := envOrDefault("E2E_NODE_IMAGE", "olricstack/olric-node:e2e")
	watchdogImage := envOrDefault("E2E_WATCHDOG_IMAGE", "olricstack/watchdog:e2e")

	env.ensureNamespace(t, ctx)
	defer env.cleanupStack(t, stackName)

	env.ensureOperatorNamespace(t, ctx)
	env.applyRepoManifest(t, ctx, env.repoPath(t, "config", "crd", "olric.io_olricstacks.yaml"), "")
	env.applyRepoManifest(t, ctx, env.repoPath(t, "config", "rbac", "operator.yaml"), "")
	env.deleteOperatorDeployment(t, ctx)
	env.applyYAMLInNamespace(t, "olric-system", fmt.Sprintf(testOperatorDeploymentYAML, envOrDefault("E2E_OPERATOR_IMAGE", "olricstack/operator:e2e")))
	env.waitForDeploymentReady(t, ctx, "olric-system", "olricstack-operator")
	env.applyYAML(t, mysqlSecretYAML)
	env.applyYAML(t, fmt.Sprintf(testStackYAML, stackName, env.namespace, nodeImage, watchdogImage))

	env.waitForStatefulSetReady(t, ctx, env.namespace, stackName+"-olric", 1)
	env.assertLeaseExists(t, ctx, env.namespace, stackName+"-watchdog")
	env.assertConfigMapHasGeneration(t, ctx, env.namespace, stackName+"-topology")
	env.assertServiceEndpoints(t, ctx, env.namespace, stackName+"-watchdog")
	env.assertServiceEndpoints(t, ctx, env.namespace, stackName+"-olric")
}

func (e *k3dEnv) ensureNamespace(t *testing.T, ctx context.Context) {
	t.Helper()

	cmd := e.kubectlCmd(ctx, "create", "namespace", e.namespace, "--dry-run=client", "-o", "yaml")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("render namespace: %v", err)
	}
	apply := e.kubectlCmd(ctx, "apply", "-f", "-")
	apply.Stdin = strings.NewReader(string(out))
	applyOut, err := apply.CombinedOutput()
	if err != nil {
		t.Fatalf("apply namespace: %v (%s)", err, strings.TrimSpace(string(applyOut)))
	}
}

func (e *k3dEnv) ensureOperatorNamespace(t *testing.T, ctx context.Context) {
	t.Helper()
	cmd := e.kubectlCmd(ctx, "create", "namespace", "olric-system", "--dry-run=client", "-o", "yaml")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("render operator namespace: %v", err)
	}
	apply := e.kubectlCmd(ctx, "apply", "-f", "-")
	apply.Stdin = strings.NewReader(string(out))
	applyOut, err := apply.CombinedOutput()
	if err != nil {
		t.Fatalf("apply operator namespace: %v (%s)", err, strings.TrimSpace(string(applyOut)))
	}
}

func (e *k3dEnv) deleteOperatorDeployment(t *testing.T, ctx context.Context) {
	t.Helper()

	cmd := e.kubectlCmd(ctx, "-n", "olric-system", "delete", "deployment", "olricstack-operator", "--ignore-not-found=true", "--wait=true")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("delete operator deployment: %v (%s)", err, strings.TrimSpace(string(out)))
	}
}

func (e *k3dEnv) applyRepoManifest(t *testing.T, ctx context.Context, path, namespace string) {
	t.Helper()

	args := []string{"apply"}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	args = append(args, "-f", path)
	cmd := e.kubectlCmd(ctx, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("apply %s: %v (%s)", path, err, strings.TrimSpace(string(out)))
	}
}

func (e *k3dEnv) cleanupStack(t *testing.T, stackName string) {
	t.Helper()
	if os.Getenv("E2E_KEEP_RESOURCES") == "1" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	commands := [][]string{
		{"delete", "olricstack", stackName, "-n", e.namespace, "--ignore-not-found=true"},
		{"delete", "statefulset", stackName + "-olric", "-n", e.namespace, "--ignore-not-found=true"},
		{"delete", "deployment", stackName + "-watchdog", "-n", e.namespace, "--ignore-not-found=true"},
		{"delete", "service", stackName + "-watchdog", stackName + "-olric", "-n", e.namespace, "--ignore-not-found=true"},
		{"delete", "serviceaccount", stackName + "-watchdog", "-n", e.namespace, "--ignore-not-found=true"},
		{"delete", "rolebinding", stackName + "-watchdog", "-n", e.namespace, "--ignore-not-found=true"},
		{"delete", "secret", "demo-mysql", "-n", e.namespace, "--ignore-not-found=true"},
		{"delete", "configmap", stackName + "-topology", "-n", e.namespace, "--ignore-not-found=true"},
	}
	for _, args := range commands {
		cmd := e.kubectlCmd(ctx, args...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Logf("cleanup %v failed: %v (%s)", args, err, strings.TrimSpace(string(out)))
		}
	}
}

func (e *k3dEnv) waitForAvailableDeployment(t *testing.T, ctx context.Context, namespace, name string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		cmd := e.kubectlCmd(ctx, "-n", namespace, "rollout", "status", "deployment/"+name, "--timeout=5s")
		out, err := cmd.CombinedOutput()
		if err == nil {
			return
		}
		if strings.Contains(string(out), `deployments.apps "`+name+`" not found`) {
			time.Sleep(time.Second)
			continue
		}
		time.Sleep(2 * time.Second)
	}
	e.dumpDiagnostics(t, namespace)
	t.Fatalf("deployment %s/%s did not become available within timeout", namespace, name)
}

func (e *k3dEnv) waitForDeploymentReady(t *testing.T, ctx context.Context, namespace, name string) {
	t.Helper()
	cmd := e.kubectlCmd(ctx, "-n", namespace, "wait", "--for=condition=available", "deployment/"+name, "--timeout=120s")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("deployment %s/%s not ready: %v (%s)", namespace, name, err, strings.TrimSpace(string(out)))
	}
}

func (e *k3dEnv) waitForStatefulSetReady(t *testing.T, ctx context.Context, namespace, name string, replicas int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		cmd := e.kubectlCmd(ctx, "-n", namespace, "get", "statefulset", name, "-o", "jsonpath={.status.readyReplicas}")
		out, err := cmd.CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) == fmt.Sprintf("%d", replicas) {
			return
		}
		time.Sleep(2 * time.Second)
	}
	e.dumpDiagnostics(t, namespace)
	t.Fatalf("statefulset %s/%s did not reach readyReplicas=%d", namespace, name, replicas)
}

func (e *k3dEnv) assertLeaseExists(t *testing.T, ctx context.Context, namespace, name string) {
	t.Helper()
	cmd := e.kubectlCmd(ctx, "-n", namespace, "get", "lease", name, "-o", "name")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("expected lease %s/%s: %v (%s)", namespace, name, err, strings.TrimSpace(string(out)))
	}
}

func (e *k3dEnv) assertConfigMapHasGeneration(t *testing.T, ctx context.Context, namespace, name string) {
	t.Helper()
	cmd := e.kubectlCmd(ctx, "-n", namespace, "get", "configmap", name, "-o", "jsonpath={.data.watchdogGeneration}")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("expected configmap %s/%s: %v (%s)", namespace, name, err, strings.TrimSpace(string(out)))
	}
	if strings.TrimSpace(string(out)) == "" {
		t.Fatalf("expected configmap %s/%s to contain watchdogGeneration", namespace, name)
	}
}

func (e *k3dEnv) assertServiceEndpoints(t *testing.T, ctx context.Context, namespace, name string) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		cmd := e.kubectlCmd(ctx, "-n", namespace, "get", "endpoints", name, "-o", "jsonpath={.subsets[*].addresses[*].ip}")
		out, err := cmd.CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) != "" {
			return
		}
		time.Sleep(time.Second)
	}
	e.dumpDiagnostics(t, namespace)
	t.Fatalf("service %s/%s has no endpoints", namespace, name)
}

func (e *k3dEnv) dumpDiagnostics(t *testing.T, namespace string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, args := range [][]string{
		{"-n", namespace, "get", "pods", "-o", "wide"},
		{"-n", namespace, "get", "events", "--sort-by=.lastTimestamp"},
		{"-n", "olric-system", "get", "pods", "-o", "wide"},
	} {
		cmd := e.kubectlCmd(ctx, args...)
		out, _ := cmd.CombinedOutput()
		t.Logf("kubectl %s\n%s", strings.Join(args, " "), strings.TrimSpace(string(out)))
	}
}

const testOperatorDeploymentYAML = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: olricstack-operator
  namespace: olric-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: olricstack-operator
  template:
    metadata:
      labels:
        app.kubernetes.io/name: olricstack-operator
    spec:
      serviceAccountName: olricstack-operator
      containers:
        - name: operator
          image: %s
          imagePullPolicy: IfNotPresent
          args:
            - --metrics-bind-address=:8080
            - --health-probe-bind-address=:8082
          ports:
            - name: health
              containerPort: 8082
          readinessProbe:
            httpGet:
              path: /readyz
              port: 8082
`

const mysqlSecretYAML = `
apiVersion: v1
kind: Secret
metadata:
  name: demo-mysql
type: Opaque
stringData:
  dsn: ""
`

const testStackYAML = `
apiVersion: olric.io/v1alpha1
kind: OlricStack
metadata:
  name: %s
  namespace: %s
spec:
  replicas: 1
  watchdogReplicas: 2
  image: %s
  watchdogImage: %s
  mysqlDsnSecret:
    name: demo-mysql
    key: dsn
`
