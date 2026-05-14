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
		Spec: olricv1alpha1.OlricStackSpec{
			Replicas: &replicas,
			MySQLDSNSecret: corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "mysql"},
				Key:                  "dsn",
			},
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stack).Build()
	controller, err := NewController(k8sClient, scheme, ControllerConfig{StackID: "demo", Namespace: "default"})
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := controller.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var statefulSet appsv1.StatefulSet
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: "demo-olric", Namespace: "default"}, &statefulSet); err != nil {
		t.Fatalf("get statefulset: %v", err)
	}
	if got := *statefulSet.Spec.Replicas; got != replicas {
		t.Fatalf("expected replicas %d, got %d", replicas, got)
	}
	if got := statefulSet.Labels[workloads.LabelStackID]; got != "demo" {
		t.Fatalf("expected stack label demo, got %q", got)
	}

	var service corev1.Service
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: "demo-olric", Namespace: "default"}, &service); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if service.Spec.ClusterIP != "None" {
		t.Fatalf("expected headless service, got clusterIP %q", service.Spec.ClusterIP)
	}
}

func TestControllerReportsPodObservations(t *testing.T) {
	scheme := newTestScheme(t)
	stack := &olricv1alpha1.OlricStack{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: olricv1alpha1.OlricStackSpec{
			MySQLDSNSecret: corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "mysql"},
				Key:                  "dsn",
			},
		},
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

type recordingObserver struct {
	stackID      string
	observations []PodObservation
}

func (o *recordingObserver) ObservePods(stackID string, observations []PodObservation) {
	o.stackID = stackID
	o.observations = append([]PodObservation(nil), observations...)
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
