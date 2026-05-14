package election

import (
	"context"
	"testing"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestElectorCreatesLeaseAsPrimary(t *testing.T) {
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	elector := newTestElector(t, k8sClient, "wd-a")

	role, generation, err := elector.TryAcquireOrRenew(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if role != topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY {
		t.Fatalf("expected primary, got %s", role)
	}
	if generation != 1 {
		t.Fatalf("expected generation 1, got %d", generation)
	}
}

func TestElectorStaysStandbyWhenLeaseIsHeld(t *testing.T) {
	now := time.Now()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(lease("demo-watchdog", "default", "wd-a", now, 1, 30*time.Second)).Build()
	elector := newTestElector(t, k8sClient, "wd-b")

	role, generation, err := elector.TryAcquireOrRenew(context.Background(), now.Add(time.Second))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if role != topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY {
		t.Fatalf("expected standby, got %s", role)
	}
	if generation != 1 {
		t.Fatalf("expected observed generation 1, got %d", generation)
	}
}

func TestElectorTakesOverExpiredLease(t *testing.T) {
	now := time.Now()
	k8sClient := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(lease("demo-watchdog", "default", "wd-a", now.Add(-time.Minute), 2, 10*time.Second)).Build()
	elector := newTestElector(t, k8sClient, "wd-b")

	role, generation, err := elector.TryAcquireOrRenew(context.Background(), now)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if role != topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY {
		t.Fatalf("expected primary, got %s", role)
	}
	if generation != 3 {
		t.Fatalf("expected generation 3, got %d", generation)
	}

	var updated coordinationv1.Lease
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: "demo-watchdog", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get lease: %v", err)
	}
	if updated.Spec.HolderIdentity == nil || *updated.Spec.HolderIdentity != "wd-b" {
		t.Fatalf("expected holder wd-b, got %#v", updated.Spec.HolderIdentity)
	}
}

func newTestElector(t *testing.T, k8sClient client.Client, identity string) *Elector {
	t.Helper()
	elector, err := NewElector(k8sClient, Config{
		LeaseName:     "demo-watchdog",
		Namespace:     "default",
		Identity:      identity,
		LeaseDuration: 10 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("new elector: %v", err)
	}
	return elector
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add coordination scheme: %v", err)
	}
	return scheme
}

func lease(name, namespace, holder string, renewTime time.Time, generation int64, duration time.Duration) *coordinationv1.Lease {
	seconds := int32(duration.Seconds())
	transitions := int32(generation)
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			RenewTime:            &metav1.MicroTime{Time: renewTime},
			LeaseDurationSeconds: &seconds,
			LeaseTransitions:     &transitions,
		},
	}
}
