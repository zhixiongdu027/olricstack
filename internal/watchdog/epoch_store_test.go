package watchdog

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestConfigMapEpochStorePersistsMonotonicEpoch(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

func TestConfigMapEpochStoreRejectsCorruptEpochOnSave(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-topology", Namespace: "default"},
		Data:       map[string]string{epochKey: "not-a-number"},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
	store := NewConfigMapEpochStore(k8sClient, "default", "demo-topology")

	if err := store.SaveEpoch(context.Background(), "demo", 7); err == nil {
		t.Fatal("expected corrupt epoch save to fail")
	}
}

func TestConfigMapEpochStoreAllocatesMonotonicGeneration(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	store := NewConfigMapEpochStore(k8sClient, "default", "demo-topology")

	first, err := store.NextGeneration(context.Background(), "demo")
	if err != nil {
		t.Fatalf("next generation first: %v", err)
	}
	second, err := store.NextGeneration(context.Background(), "demo")
	if err != nil {
		t.Fatalf("next generation second: %v", err)
	}
	if first != 1 || second != 2 {
		t.Fatalf("expected generations 1 and 2, got %d and %d", first, second)
	}
}

func TestConfigMapEpochStoreRetriesConflictsOnSave(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	store := NewConfigMapEpochStore(&conflictingClient{Client: baseClient}, "default", "demo-topology")

	if err := store.SaveEpoch(context.Background(), "demo", 7); err != nil {
		t.Fatalf("save epoch 7: %v", err)
	}
	if err := store.SaveEpoch(context.Background(), "demo", 9); err != nil {
		t.Fatalf("save epoch 9 with conflict retry: %v", err)
	}
	epoch, err := store.LoadEpoch(context.Background(), "demo")
	if err != nil {
		t.Fatalf("load epoch: %v", err)
	}
	if epoch != 9 {
		t.Fatalf("expected epoch 9 after retry, got %d", epoch)
	}
}

func TestConfigMapEpochStoreRejectsCorruptGeneration(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-topology", Namespace: "default"},
		Data:       map[string]string{generationKey: "bad"},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
	store := NewConfigMapEpochStore(k8sClient, "default", "demo-topology")

	if _, err := store.NextGeneration(context.Background(), "demo"); err == nil {
		t.Fatal("expected corrupt generation allocation to fail")
	}
}

type conflictingClient struct {
	client.Client
	updateCount int
}

func (c *conflictingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.updateCount++
	if c.updateCount == 1 {
		return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, obj.GetName(), errors.New("conflict"))
	}
	return c.Client.Update(ctx, obj, opts...)
}
