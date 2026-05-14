package watchdog

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestConfigMapEpochStorePersistsMonotonicEpoch(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	store := NewConfigMapEpochStore(k8sClient, "default", "demo-topology")

	if err := store.SaveEpoch(context.Background(), "demo", 7); err != nil {
		t.Fatalf("save epoch 7: %v", err)
	}
	if err := store.SaveEpoch(context.Background(), "demo", 3); err != nil {
		t.Fatalf("save epoch 3: %v", err)
	}
	epoch, err := store.LoadEpoch(context.Background(), "demo")
	if err != nil {
		t.Fatalf("load epoch: %v", err)
	}
	if epoch != 7 {
		t.Fatalf("expected monotonic epoch 7, got %d", epoch)
	}
}

func TestConfigMapEpochStoreTreatsMissingEpochAsZero(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "demo-topology", Namespace: "default"}}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
	store := NewConfigMapEpochStore(k8sClient, "default", "demo-topology")

	epoch, err := store.LoadEpoch(context.Background(), "demo")
	if err != nil {
		t.Fatalf("load epoch: %v", err)
	}
	if epoch != 0 {
		t.Fatalf("expected missing epoch to load as zero, got %d", epoch)
	}
}
