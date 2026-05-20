package watchdog

import (
	"context"
	"testing"

	olricv1alpha1 "github.com/zhixiongdu/olricstack/api/olric/v1alpha1"
	"github.com/zhixiongdu/olricstack/internal/workloads"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestControllerReconcilesOlricResourcesForOwnStack(t *testing.T) {
	scheme := newTestScheme(t)
	replicas := int32(2)
	stack := &olricv1alpha1.OlricStack{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "olric.io/v1alpha1",
			Kind:       "OlricStack",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
		},
		Spec: olricv1alpha1.OlricStackSpec{Replicas: &replicas},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stack).Build()
	controller, err := NewController(k8sClient, scheme, ControllerConfig{StackID: "demo", Namespace: "default"})
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := controller.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var deployment appsv1.Deployment
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: "demo-olric", Namespace: "default"}, &deployment); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if got := *deployment.Spec.Replicas; got != replicas {
		t.Fatalf("expected replicas %d, got %d", replicas, got)
	}
	if got := deployment.Labels[workloads.LabelStackID]; got != "demo" {
		t.Fatalf("expected stack label demo, got %q", got)
	}
	if len(deployment.Spec.Template.Spec.Volumes) != 1 || deployment.Spec.Template.Spec.Volumes[0].Name != workloads.SharedVolumeName {
		t.Fatalf("expected shared tmpfs volume, got %#v", deployment.Spec.Template.Spec.Volumes)
	}
	if deployment.Spec.Template.Spec.Volumes[0].EmptyDir == nil || deployment.Spec.Template.Spec.Volumes[0].EmptyDir.Medium != corev1.StorageMediumMemory {
		t.Fatalf("expected shared volume to be tmpfs, got %#v", deployment.Spec.Template.Spec.Volumes[0].EmptyDir)
	}
	if len(deployment.Spec.Template.Spec.Containers) != 2 {
		t.Fatalf("expected node + sidecar containers, got %d", len(deployment.Spec.Template.Spec.Containers))
	}
	nodeContainer := deployment.Spec.Template.Spec.Containers[0]
	if nodeContainer.Name != "olric-node" {
		t.Fatalf("expected first container olric-node, got %q", nodeContainer.Name)
	}
	if len(nodeContainer.VolumeMounts) != 1 || nodeContainer.VolumeMounts[0].Name != workloads.SharedVolumeName {
		t.Fatalf("expected node to mount shared volume only, got %#v", nodeContainer.VolumeMounts)
	}
	ports := nodeContainer.Ports
	if len(ports) != 3 ||
		ports[0].Name != "olric-internal" || ports[0].ContainerPort != workloads.OlricPort ||
		ports[1].Name != "resp" || ports[1].ContainerPort != workloads.RESPPort ||
		ports[2].Name != "memberlist" || ports[2].ContainerPort != workloads.MemberlistPort {
		t.Fatalf("expected Olric internal, RESP and memberlist ports, got %#v", ports)
	}
	env := envMap(nodeContainer.Env)
	if env["OLRIC_BIND_PORT"].Value != "3320" || env["RESP_BIND_PORT"].Value != "3321" || env["OLRIC_MEMBERLIST_BIND_PORT"].Value != "3322" {
		t.Fatalf("expected Olric, RESP and memberlist port env vars, got %#v", env)
	}
	if env["RING_PATH"].Value == "" || env["CONTROL_SOCKET"].Value == "" {
		t.Fatalf("expected RING_PATH and CONTROL_SOCKET env, got %#v", env)
	}

	sidecarContainer := deployment.Spec.Template.Spec.Containers[1]
	if sidecarContainer.Name != "olric-sidecar" {
		t.Fatalf("expected second container olric-sidecar, got %q", sidecarContainer.Name)
	}

	var service corev1.Service
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: "demo-olric", Namespace: "default"}, &service); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if service.Annotations[workloads.AnnotationServiceScope] != workloads.ServiceScopeInternal {
		t.Fatalf("expected internal governing service annotation, got %#v", service.Annotations)
	}
	if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Name != "resp" || service.Spec.Ports[0].Port != workloads.RESPPort {
		t.Fatalf("expected RESP service port only, got %#v", service.Spec.Ports)
	}
}

func TestControllerReportsPodObservations(t *testing.T) {
	scheme := newTestScheme(t)
	stack := &olricv1alpha1.OlricStack{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: olricv1alpha1.OlricStackSpec{},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-olric-0",
			Namespace: "default",
			Labels:    workloads.StackLabels(stack, workloads.ComponentOlricNode),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.2",
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}

	observer := &recordingObserver{}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stack, pod).Build()
	controller, err := NewController(k8sClient, scheme, ControllerConfig{StackID: "demo", Namespace: "default"})
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	controller.Observer = observer
	if err := controller.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if observer.stackID != "demo" {
		t.Fatalf("expected stack id demo, got %q", observer.stackID)
	}
	if len(observer.observations) != 1 {
		t.Fatalf("expected one observation, got %d", len(observer.observations))
	}
	if got := observer.observations[0]; got.PodIP != "10.0.0.2" || !got.Ready || got.Phase != string(corev1.PodRunning) {
		t.Fatalf("unexpected observation: %#v", got)
	}
}

func TestControllerPreservesExistingDeploymentImmutableFields(t *testing.T) {
	scheme := newTestScheme(t)
	replicas := int32(2)
	stack := &olricv1alpha1.OlricStack{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: olricv1alpha1.OlricStackSpec{
			Replicas: &replicas,
			MySQLDSNSecret: corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "mysql"},
				Key:                  "dsn",
			},
		},
	}
	existing := workloads.OlricDeployment(stack)
	customSelector := existing.Spec.Selector.DeepCopy()
	customSelector.MatchLabels["extra"] = "unchanged"

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stack, existing).Build()
	controller, err := NewController(k8sClient, scheme, ControllerConfig{StackID: "demo", Namespace: "default"})
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	// Mutate the in-cluster selector so the test verifies preservation.
	var stored appsv1.Deployment
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: "demo-olric", Namespace: "default"}, &stored); err != nil {
		t.Fatalf("get stored deployment: %v", err)
	}
	stored.Spec.Selector = customSelector
	if err := k8sClient.Update(context.Background(), &stored); err != nil {
		t.Fatalf("update stored deployment: %v", err)
	}
	if err := controller.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var updated appsv1.Deployment
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: "demo-olric", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if got := updated.Spec.Selector.MatchLabels["extra"]; got != "unchanged" {
		t.Fatalf("expected existing selector to be preserved, got %q", got)
	}
	if got := *updated.Spec.Replicas; got != replicas {
		t.Fatalf("expected mutable replicas to update to %d, got %d", replicas, got)
	}
}

type recordingObserver struct {
	stackID      string
	observations []PodObservation
}

func (o *recordingObserver) ObservePods(stackID string, observations []PodObservation) {
	o.stackID = stackID
	o.observations = append([]PodObservation(nil), observations...)
}

func envMap(envVars []corev1.EnvVar) map[string]corev1.EnvVar {
	env := make(map[string]corev1.EnvVar, len(envVars))
	for _, envVar := range envVars {
		env[envVar.Name] = envVar
	}
	return env
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := olricv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add olric scheme: %v", err)
	}
	return scheme
}
