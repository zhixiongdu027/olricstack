package operator

import (
	"context"
	"testing"

	olricv1alpha1 "github.com/zhixiongdu/olricstack/api/olric/v1alpha1"
	"github.com/zhixiongdu/olricstack/internal/workloads"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcileCreatesStackResources(t *testing.T) {
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

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(stack).
		WithStatusSubresource(&olricv1alpha1.OlricStack{}).
		Build()
	reconciler := &OlricStackReconciler{Client: client, Scheme: scheme}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var watchdogService corev1.Service
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-watchdog", Namespace: "default"}, &watchdogService); err != nil {
		t.Fatalf("get watchdog service: %v", err)
	}
	if got := watchdogService.Spec.Selector[LabelStackID]; got != "demo" {
		t.Fatalf("expected watchdog service stack selector demo, got %q", got)
	}

	var watchdog appsv1.Deployment
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-watchdog", Namespace: "default"}, &watchdog); err != nil {
		t.Fatalf("get watchdog deployment: %v", err)
	}
	watchdogEnv := envMap(watchdog.Spec.Template.Spec.Containers[0].Env)
	if watchdogEnv["STACK_ID"].Value != "demo" {
		t.Fatalf("expected STACK_ID demo, got %q", watchdogEnv["STACK_ID"].Value)
	}
	if watchdogEnv["BOOKWORM_INTERVAL"].Value != "10s" {
		t.Fatalf("expected BOOKWORM_INTERVAL 10s, got %q", watchdogEnv["BOOKWORM_INTERVAL"].Value)
	}
	readiness := watchdog.Spec.Template.Spec.Containers[0].ReadinessProbe
	if readiness == nil || readiness.GRPC == nil || readiness.GRPC.Port != int32(8081) {
		t.Fatalf("expected watchdog gRPC readiness probe, got %#v", readiness)
	}
	rolling := watchdog.Spec.Strategy.RollingUpdate
	if rolling == nil || rolling.MaxUnavailable == nil || rolling.MaxUnavailable.IntValue() != 1 {
		t.Fatalf("expected watchdog rolling update to allow one unavailable standby, got %#v", rolling)
	}
	watchdogServiceAccountName := workloads.WatchdogServiceAccountName(stack)
	if watchdog.Spec.Template.Spec.ServiceAccountName != watchdogServiceAccountName {
		t.Fatalf("expected watchdog service account %q, got %q", watchdogServiceAccountName, watchdog.Spec.Template.Spec.ServiceAccountName)
	}

	var watchdogSA corev1.ServiceAccount
	if err := client.Get(context.Background(), types.NamespacedName{Name: watchdogServiceAccountName, Namespace: "default"}, &watchdogSA); err != nil {
		t.Fatalf("get watchdog service account: %v", err)
	}

	var watchdogRoleBinding rbacv1.RoleBinding
	if err := client.Get(context.Background(), types.NamespacedName{Name: watchdogServiceAccountName, Namespace: "default"}, &watchdogRoleBinding); err != nil {
		t.Fatalf("get watchdog role binding: %v", err)
	}
	if watchdogRoleBinding.RoleRef.Kind != "ClusterRole" || watchdogRoleBinding.RoleRef.Name != workloads.WatchdogClusterRoleName {
		t.Fatalf("expected watchdog role binding to target %q, got %#v", workloads.WatchdogClusterRoleName, watchdogRoleBinding.RoleRef)
	}
	if len(watchdogRoleBinding.Subjects) != 1 || watchdogRoleBinding.Subjects[0].Kind != "ServiceAccount" || watchdogRoleBinding.Subjects[0].Name != watchdogServiceAccountName || watchdogRoleBinding.Subjects[0].Namespace != "default" {
		t.Fatalf("unexpected watchdog role binding subjects: %#v", watchdogRoleBinding.Subjects)
	}

	var olricDeployment appsv1.Deployment
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-olric", Namespace: "default"}, &olricDeployment); err == nil {
		t.Fatal("global operator should not create olric deployment directly")
	}
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
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add rbac scheme: %v", err)
	}
	if err := olricv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add olric scheme: %v", err)
	}
	return scheme
}

func envMap(envVars []corev1.EnvVar) map[string]corev1.EnvVar {
	env := make(map[string]corev1.EnvVar, len(envVars))
	for _, envVar := range envVars {
		env[envVar.Name] = envVar
	}
	return env
}
