//go:build e2e

package e2e

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func base64DSN(dsn string) string {
	return base64.StdEncoding.EncodeToString([]byte(dsn))
}

func TestKindStackReconcilesThroughWatchdog(t *testing.T) {
	env := requireKindEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	stackName := envOrDefault("E2E_STACK_NAME", "demo")
	defer env.cleanupStack(t, stackName)
	deployTestStack(t, ctx, env, stackName, testStackOptions{})
}

func TestKindWatchdogFailoverReconnectsOlricNode(t *testing.T) {
	env := requireKindEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stackName := envOrDefault("E2E_STACK_NAME", "demo") + "-failover"
	defer env.cleanupStack(t, stackName)
	deployTestStack(t, ctx, env, stackName, testStackOptions{})

	leaseName := stackName + "-watchdog"
	configMapName := stackName + "-topology"
	primaryPod, err := env.readLeaseHolder(ctx, env.namespace, leaseName)
	if err != nil {
		t.Fatalf("read primary watchdog lease holder: %v", err)
	}
	if primaryPod == "" {
		t.Fatal("expected watchdog lease holder to be set")
	}

	initialGeneration, err := env.readConfigMapGeneration(ctx, env.namespace, configMapName)
	if err != nil {
		t.Fatalf("read initial watchdog generation: %v", err)
	}
	if initialGeneration <= 0 {
		t.Fatalf("expected positive initial watchdog generation, got %d", initialGeneration)
	}

	nodePodName := env.firstNodePod(t, ctx, stackName)
	initialToken := fmt.Sprintf("watchdog=%s/%d", primaryPod, initialGeneration)
	env.waitForNodeLogContains(t, ctx, env.namespace, nodePodName, initialToken)

	cmd := env.kubectlCmd(ctx, "-n", env.namespace, "delete", "pod", primaryPod, "--wait=true")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("delete primary watchdog pod %s: %v (%s)", primaryPod, err, strings.TrimSpace(string(out)))
	}

	nextPrimaryPod := env.waitForLeaseHolderChange(t, ctx, env.namespace, leaseName, primaryPod)
	env.waitForPodReady(t, ctx, env.namespace, nextPrimaryPod)

	nextGeneration := env.waitForConfigMapGenerationGreater(t, ctx, env.namespace, configMapName, initialGeneration)
	env.assertServiceEndpoints(t, ctx, env.namespace, leaseName)

	nextToken := fmt.Sprintf("watchdog=%s/%d", nextPrimaryPod, nextGeneration)
	env.waitForNodeLogContains(t, ctx, env.namespace, nodePodName, nextToken)
}

func TestKindMySQLDurableWritePath(t *testing.T) {
	env := requireKindEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	stackName := envOrDefault("E2E_STACK_NAME", "demo") + "-mysql"
	defer env.cleanupStack(t, stackName)

	deployTestStack(t, ctx, env, stackName, testStackOptions{})

	nodePodName := env.firstNodePod(t, ctx, stackName)
	env.waitForNodeLogContains(t, ctx, env.namespace, nodePodName, "olric node bootstrap complete")
	env.waitForSidecarLogContains(t, ctx, env.namespace, nodePodName, "sidecar bootstrap complete")

	writeCtx, writeCancel := context.WithTimeout(ctx, 15*time.Second)
	defer writeCancel()

	key := "user:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := env.writeDMapFromNode(t, writeCtx, env.namespace, nodePodName, "users", key, "value-"+stackName); err != nil {
		t.Fatalf("write durable dmap entry: %v", err)
	}
}

// testStackOptions controls the OlricStack manifest applied by deployTestStack.
// Defaults are filled in by withDefaults so each test only specifies what
// matters to it.
type testStackOptions struct {
	mysqlSecretName string
	mysqlDSN        string
	sidecarImage    string
}

func (o testStackOptions) withDefaults(stackName string) testStackOptions {
	if o.mysqlSecretName == "" {
		o.mysqlSecretName = stackName + "-mysql"
	}
	if o.sidecarImage == "" {
		o.sidecarImage = envOrDefault("E2E_SIDECAR_IMAGE", "olricstack/olric-sidecar:e2e")
	}
	return o
}

func deployTestStack(t *testing.T, ctx context.Context, env *kindEnv, stackName string, opts testStackOptions) {
	t.Helper()

	opts = opts.withDefaults(stackName)
	nodeImage := envOrDefault("E2E_NODE_IMAGE", "olricstack/olric-node:e2e")
	watchdogImage := envOrDefault("E2E_WATCHDOG_IMAGE", "olricstack/watchdog:e2e")
	if opts.mysqlDSN == "" {
		mysqlHost := envOrDefault("E2E_MYSQL_HOST", env.hostGateway(t))
		mysqlPort := envOrDefault("E2E_MYSQL_PORT", "3306")
		mysqlUser := envOrDefault("E2E_MYSQL_USER", "root")
		mysqlPassword := envOrDefault("E2E_MYSQL_PASSWORD", "password")
		mysqlDatabase := envOrDefault("E2E_MYSQL_DATABASE", "olric_e2e")
		opts.mysqlDSN = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", mysqlUser, mysqlPassword, mysqlHost, mysqlPort, mysqlDatabase)
	}

	env.ensureNamespace(t, ctx)
	env.ensureOperatorNamespace(t, ctx)
	env.applyRepoManifest(t, ctx, env.repoPath(t, "config", "crd", "olric.io_olricstacks.yaml"), "")
	env.applyRepoManifest(t, ctx, env.repoPath(t, "config", "rbac", "operator.yaml"), "")
	env.deleteOperatorDeployment(t, ctx)
	env.applyYAMLInNamespace(t, "olric-system", fmt.Sprintf(testOperatorDeploymentYAML, envOrDefault("E2E_OPERATOR_IMAGE", "olricstack/operator:e2e")))
	env.waitForDeploymentReady(t, ctx, "olric-system", "olricstack-operator")
	env.applyYAML(t, fmt.Sprintf(testMySQLSecretYAML, opts.mysqlSecretName, env.namespace, base64DSN(opts.mysqlDSN)))
	env.applyYAML(t, fmt.Sprintf(testStackYAML, stackName, env.namespace, nodeImage, opts.sidecarImage, watchdogImage, opts.mysqlSecretName))

	env.waitForDeploymentReady(t, ctx, env.namespace, stackName+"-olric")
	env.assertLeaseExists(t, ctx, env.namespace, stackName+"-watchdog")
	env.assertConfigMapHasGeneration(t, ctx, env.namespace, stackName+"-topology")
	env.assertServiceEndpoints(t, ctx, env.namespace, stackName+"-watchdog")
	env.assertServiceEndpoints(t, ctx, env.namespace, stackName+"-olric")
}

// firstNodePod returns the name of the first olric-node pod for stack, waiting
// briefly for the Deployment to materialise pods.
func (e *kindEnv) firstNodePod(t *testing.T, ctx context.Context, stackName string) string {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		cmd := e.kubectlCmd(ctx, "-n", e.namespace, "get", "pods",
			"-l", "app.kubernetes.io/component=olric-node,olric.io/stack-id="+stackName,
			"-o", "jsonpath={.items[0].metadata.name}")
		out, err := cmd.CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) != "" {
			return strings.TrimSpace(string(out))
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("no olric-node pod found for stack %s", stackName)
	return ""
}

func (e *kindEnv) ensureNamespace(t *testing.T, ctx context.Context) {
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

func (e *kindEnv) ensureOperatorNamespace(t *testing.T, ctx context.Context) {
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

func (e *kindEnv) deleteOperatorDeployment(t *testing.T, ctx context.Context) {
	t.Helper()

	cmd := e.kubectlCmd(ctx, "-n", "olric-system", "delete", "deployment", "olricstack-operator", "--ignore-not-found=true", "--wait=true")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("delete operator deployment: %v (%s)", err, strings.TrimSpace(string(out)))
	}
}

func (e *kindEnv) applyRepoManifest(t *testing.T, ctx context.Context, path, namespace string) {
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

func (e *kindEnv) cleanupStack(t *testing.T, stackName string) {
	t.Helper()
	if os.Getenv("E2E_KEEP_RESOURCES") == "1" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	commands := [][]string{
		{"delete", "olricstack", stackName, "-n", e.namespace, "--ignore-not-found=true"},
		{"delete", "deployment", stackName + "-olric", "-n", e.namespace, "--ignore-not-found=true"},
		{"delete", "deployment", stackName + "-watchdog", "-n", e.namespace, "--ignore-not-found=true"},
		{"delete", "service", stackName + "-watchdog", stackName + "-olric", "-n", e.namespace, "--ignore-not-found=true"},
		{"delete", "serviceaccount", stackName + "-watchdog", "-n", e.namespace, "--ignore-not-found=true"},
		{"delete", "rolebinding", stackName + "-watchdog", "-n", e.namespace, "--ignore-not-found=true"},
		{"delete", "configmap", stackName + "-topology", "-n", e.namespace, "--ignore-not-found=true"},
		{"delete", "secret", stackName + "-mysql", "-n", e.namespace, "--ignore-not-found=true"},
	}
	for _, args := range commands {
		cmd := e.kubectlCmd(ctx, args...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Logf("cleanup %v failed: %v (%s)", args, err, strings.TrimSpace(string(out)))
		}
	}
}

func (e *kindEnv) waitForAvailableDeployment(t *testing.T, ctx context.Context, namespace, name string) {
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

func (e *kindEnv) waitForDeploymentReady(t *testing.T, ctx context.Context, namespace, name string) {
	t.Helper()
	cmd := e.kubectlCmd(ctx, "-n", namespace, "wait", "--for=condition=available", "deployment/"+name, "--timeout=120s")
	out, err := cmd.CombinedOutput()
	if err != nil {
		e.dumpDiagnostics(t, namespace)
		t.Fatalf("deployment %s/%s not ready: %v (%s)", namespace, name, err, strings.TrimSpace(string(out)))
	}
}

func (e *kindEnv) assertLeaseExists(t *testing.T, ctx context.Context, namespace, name string) {
	t.Helper()
	cmd := e.kubectlCmd(ctx, "-n", namespace, "get", "lease", name, "-o", "name")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("expected lease %s/%s: %v (%s)", namespace, name, err, strings.TrimSpace(string(out)))
	}
}

func (e *kindEnv) assertConfigMapHasGeneration(t *testing.T, ctx context.Context, namespace, name string) {
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

func (e *kindEnv) assertServiceEndpoints(t *testing.T, ctx context.Context, namespace, name string) {
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

func (e *kindEnv) readLeaseHolder(ctx context.Context, namespace, name string) (string, error) {
	cmd := e.kubectlCmd(ctx, "-n", namespace, "get", "lease", name, "-o", "jsonpath={.spec.holderIdentity}")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("get lease holder for %s/%s: %w (%s)", namespace, name, err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

func (e *kindEnv) waitForLeaseHolderChange(t *testing.T, ctx context.Context, namespace, name, previous string) string {
	t.Helper()

	deadline := time.Now().Add(90 * time.Second)
	var lastHolder string
	for time.Now().Before(deadline) {
		holder, err := e.readLeaseHolder(ctx, namespace, name)
		if err == nil && holder != "" && holder != previous {
			return holder
		}
		if err == nil {
			lastHolder = holder
		}
		time.Sleep(2 * time.Second)
	}
	e.dumpDiagnostics(t, namespace)
	t.Fatalf("lease holder for %s/%s did not change from %q (last observed %q)", namespace, name, previous, lastHolder)
	return ""
}

func (e *kindEnv) readConfigMapGeneration(ctx context.Context, namespace, name string) (int64, error) {
	cmd := e.kubectlCmd(ctx, "-n", namespace, "get", "configmap", name, "-o", "jsonpath={.data.watchdogGeneration}")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("get configmap generation for %s/%s: %w (%s)", namespace, name, err, strings.TrimSpace(string(out)))
	}
	value := strings.TrimSpace(string(out))
	if value == "" {
		return 0, fmt.Errorf("configmap %s/%s has empty watchdogGeneration", namespace, name)
	}
	generation, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse watchdogGeneration %q for %s/%s: %w", value, namespace, name, err)
	}
	return generation, nil
}

func (e *kindEnv) waitForConfigMapGenerationGreater(t *testing.T, ctx context.Context, namespace, name string, previous int64) int64 {
	t.Helper()

	deadline := time.Now().Add(90 * time.Second)
	var lastGeneration int64
	for time.Now().Before(deadline) {
		generation, err := e.readConfigMapGeneration(ctx, namespace, name)
		if err == nil && generation > previous {
			return generation
		}
		if err == nil {
			lastGeneration = generation
		}
		time.Sleep(2 * time.Second)
	}
	e.dumpDiagnostics(t, namespace)
	t.Fatalf("configmap %s/%s generation did not advance past %d (last observed %d)", namespace, name, previous, lastGeneration)
	return 0
}

func (e *kindEnv) waitForPodReady(t *testing.T, ctx context.Context, namespace, name string) {
	t.Helper()

	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		cmd := e.kubectlCmd(ctx, "-n", namespace, "wait", "--for=condition=ready", "pod/"+name, "--timeout=5s")
		if out, err := cmd.CombinedOutput(); err == nil {
			_ = out
			return
		}
		time.Sleep(2 * time.Second)
	}
	e.dumpDiagnostics(t, namespace)
	t.Fatalf("pod %s/%s did not become ready within timeout", namespace, name)
}

func (e *kindEnv) waitForNodeLogContains(t *testing.T, ctx context.Context, namespace, podName, needle string) {
	t.Helper()

	deadline := time.Now().Add(90 * time.Second)
	var lastLogs string
	for time.Now().Before(deadline) {
		cmd := e.kubectlCmd(ctx, "-n", namespace, "logs", podName, "-c", "olric-node")
		out, err := cmd.CombinedOutput()
		if err == nil {
			logs := string(out)
			if strings.Contains(logs, needle) {
				return
			}
			lastLogs = logs
		}
		time.Sleep(2 * time.Second)
	}
	e.dumpDiagnostics(t, namespace)
	t.Fatalf("pod %s/%s logs did not contain %q; last logs:\n%s", namespace, podName, needle, strings.TrimSpace(lastLogs))
}

func (e *kindEnv) waitForSidecarLogContains(t *testing.T, ctx context.Context, namespace, podName, needle string) {
	t.Helper()

	deadline := time.Now().Add(90 * time.Second)
	var lastLogs string
	for time.Now().Before(deadline) {
		cmd := e.kubectlCmd(ctx, "-n", namespace, "logs", podName, "-c", "olric-sidecar")
		out, err := cmd.CombinedOutput()
		if err == nil {
			logs := string(out)
			if strings.Contains(logs, needle) {
				return
			}
			lastLogs = logs
		}
		time.Sleep(2 * time.Second)
	}
	e.dumpDiagnostics(t, namespace)
	t.Fatalf("pod %s/%s sidecar logs did not contain %q; last logs:\n%s", namespace, podName, needle, strings.TrimSpace(lastLogs))
}

func (e *kindEnv) writeDMapFromNode(t *testing.T, ctx context.Context, namespace, podName, dmap, key, value string) error {
	t.Helper()

	podIP, err := e.readPodField(ctx, namespace, podName, "{.status.podIP}")
	if err != nil {
		return err
	}
	nodeName, err := e.readPodField(ctx, namespace, podName, "{.spec.nodeName}")
	if err != nil {
		return err
	}

	localBinary := filepath.Join(t.TempDir(), "olric-e2e-client")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", localBinary, "./cmd/olric-e2e-client")
	buildCmd.Dir = e.repoRoot(t)
	buildCmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("build olric e2e client: %w (%s)", err, strings.TrimSpace(string(out)))
	}

	nodeContainer := kindNodeContainerName(nodeName)
	remoteBinary := "/tmp/olric-e2e-client"
	copyCmd := exec.CommandContext(ctx, "docker", "cp", localBinary, nodeContainer+":"+remoteBinary)
	if out, err := copyCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("copy olric e2e client to %s: %w (%s)", nodeContainer, err, strings.TrimSpace(string(out)))
	}

	cmd := exec.CommandContext(ctx,
		"docker", "exec", nodeContainer,
		remoteBinary,
		"-addr", podIP+":3321",
		"-dmap", dmap,
		"-key", key,
		"-value", value,
		"-timeout", "5s",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("write dmap from node %s to %s: %w (%s)", nodeName, podIP, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (e *kindEnv) readPodField(ctx context.Context, namespace, podName, jsonPath string) (string, error) {
	cmd := e.kubectlCmd(ctx, "-n", namespace, "get", "pod", podName, "-o", "jsonpath="+jsonPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("get pod field %s for %s/%s: %w (%s)", jsonPath, namespace, podName, err, strings.TrimSpace(string(out)))
	}
	value := strings.TrimSpace(string(out))
	if value == "" {
		return "", fmt.Errorf("pod field %s for %s/%s is empty", jsonPath, namespace, podName)
	}
	return value, nil
}

func (e *kindEnv) hostGateway(t *testing.T) string {
	t.Helper()

	cmd := exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}", kindControlPlaneContainerName(e.clusterName))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("read kind host gateway: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	gateway := strings.TrimSpace(string(out))
	if gateway == "" {
		t.Fatal("kind host gateway is empty")
	}
	return gateway
}

func kindNodeContainerName(nodeName string) string {
	return nodeName
}

func kindControlPlaneContainerName(clusterName string) string {
	return clusterName + "-control-plane"
}

func (e *kindEnv) dumpDiagnostics(t *testing.T, namespace string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, args := range [][]string{
		{"-n", namespace, "get", "pods", "-o", "wide"},
		{"-n", namespace, "get", "events", "--sort-by=.lastTimestamp"},
		{"-n", namespace, "get", "svc"},
		{"-n", namespace, "get", "deploy"},
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
  sidecarImage: %s
  watchdogImage: %s
  mysqlDsnSecret:
    name: %s
    key: dsn
`

const testMySQLSecretYAML = `
apiVersion: v1
kind: Secret
metadata:
  name: %s
  namespace: %s
type: Opaque
data:
  dsn: %s
`
