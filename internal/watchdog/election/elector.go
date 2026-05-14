package election

import (
	"context"
	"errors"
	"time"

	topologypb "github.com/zhixiongdu/olricstack/api/topology/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Config struct {
	LeaseName       string
	Namespace       string
	Identity        string
	LeaseDuration   time.Duration
	RenewInterval   time.Duration
	AcquireInterval time.Duration
}

func (c Config) withDefaults() Config {
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 15 * time.Second
	}
	if c.RenewInterval <= 0 {
		c.RenewInterval = c.LeaseDuration / 3
	}
	if c.AcquireInterval <= 0 {
		c.AcquireInterval = c.RenewInterval
	}
	return c
}

type Observer interface {
	SetLeadership(role topologypb.WatchdogRole, generation int64)
}

type Elector struct {
	client   client.Client
	cfg      Config
	observer Observer
}

func NewElector(k8sClient client.Client, cfg Config, observer Observer) (*Elector, error) {
	cfg = cfg.withDefaults()
	if k8sClient == nil {
		return nil, errors.New("kubernetes client is nil")
	}
	if cfg.LeaseName == "" {
		return nil, errors.New("lease name is required")
	}
	if cfg.Namespace == "" {
		return nil, errors.New("namespace is required")
	}
	if cfg.Identity == "" {
		return nil, errors.New("identity is required")
	}
	return &Elector{client: k8sClient, cfg: cfg, observer: observer}, nil
}

func (e *Elector) Run(ctx context.Context) {
	for {
		role := e.step(ctx)
		wait := e.cfg.AcquireInterval
		if role == topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY {
			wait = e.cfg.RenewInterval
		}
		select {
		case <-ctx.Done():
			e.publish(topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, 0)
			return
		case <-time.After(wait):
		}
	}
}

func (e *Elector) step(ctx context.Context) topologypb.WatchdogRole {
	role, generation, err := e.TryAcquireOrRenew(ctx, time.Now())
	if err != nil {
		e.publish(topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, 0)
		return topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY
	}
	e.publish(role, generation)
	return role
}

func (e *Elector) TryAcquireOrRenew(ctx context.Context, now time.Time) (topologypb.WatchdogRole, int64, error) {
	var lease coordinationv1.Lease
	key := types.NamespacedName{Name: e.cfg.LeaseName, Namespace: e.cfg.Namespace}
	if err := e.client.Get(ctx, key, &lease); err != nil {
		if !apierrors.IsNotFound(err) {
			return topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, 0, err
		}
		created := e.newLease(now, 1)
		if err := e.client.Create(ctx, created); err != nil {
			return topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, 0, err
		}
		return topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY, 1, nil
	}

	holder := value(lease.Spec.HolderIdentity)
	generation := int64(value(lease.Spec.LeaseTransitions))
	expired := leaseExpired(&lease, now)
	if holder == e.cfg.Identity {
		lease.Spec.RenewTime = &metav1.MicroTime{Time: now}
		if lease.Spec.LeaseDurationSeconds == nil {
			seconds := int32(e.cfg.LeaseDuration.Seconds())
			lease.Spec.LeaseDurationSeconds = &seconds
		}
		if err := e.client.Update(ctx, &lease); err != nil {
			return topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, generation, err
		}
		return topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY, generation, nil
	}

	if expired {
		nextGeneration := generation + 1
		identity := e.cfg.Identity
		lease.Spec.HolderIdentity = &identity
		lease.Spec.RenewTime = &metav1.MicroTime{Time: now}
		lease.Spec.AcquireTime = &metav1.MicroTime{Time: now}
		transitions := int32(nextGeneration)
		lease.Spec.LeaseTransitions = &transitions
		seconds := int32(e.cfg.LeaseDuration.Seconds())
		lease.Spec.LeaseDurationSeconds = &seconds
		if err := e.client.Update(ctx, &lease); err != nil {
			return topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, generation, err
		}
		return topologypb.WatchdogRole_WATCHDOG_ROLE_PRIMARY, nextGeneration, nil
	}

	return topologypb.WatchdogRole_WATCHDOG_ROLE_STANDBY, generation, nil
}

func (e *Elector) newLease(now time.Time, generation int64) *coordinationv1.Lease {
	identity := e.cfg.Identity
	seconds := int32(e.cfg.LeaseDuration.Seconds())
	transitions := int32(generation)
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      e.cfg.LeaseName,
			Namespace: e.cfg.Namespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &identity,
			AcquireTime:          &metav1.MicroTime{Time: now},
			RenewTime:            &metav1.MicroTime{Time: now},
			LeaseDurationSeconds: &seconds,
			LeaseTransitions:     &transitions,
		},
	}
}

func (e *Elector) publish(role topologypb.WatchdogRole, generation int64) {
	if e.observer != nil {
		e.observer.SetLeadership(role, generation)
	}
}

func leaseExpired(lease *coordinationv1.Lease, now time.Time) bool {
	if lease.Spec.RenewTime == nil {
		return true
	}
	duration := 15 * time.Second
	if lease.Spec.LeaseDurationSeconds != nil {
		duration = time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	}
	return lease.Spec.RenewTime.Add(duration).Before(now)
}

func value[T comparable](ptr *T) T {
	var zero T
	if ptr == nil {
		return zero
	}
	return *ptr
}
