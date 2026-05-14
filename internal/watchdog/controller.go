package watchdog

import (
	"context"
	"errors"
	"sort"
	"time"

	olricv1alpha1 "github.com/zhixiongdu/olricstack/api/olric/v1alpha1"
	"github.com/zhixiongdu/olricstack/internal/workloads"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type ControllerConfig struct {
	StackID           string
	Namespace         string
	ReconcileInterval time.Duration
	ErrorBackoff      time.Duration
}

func (c ControllerConfig) withDefaults() ControllerConfig {
	if c.Namespace == "" {
		c.Namespace = "default"
	}
	if c.ReconcileInterval <= 0 {
		c.ReconcileInterval = 10 * time.Second
	}
	if c.ErrorBackoff <= 0 {
		c.ErrorBackoff = c.ReconcileInterval
	}
	return c
}

type Controller struct {
	client.Client
	Scheme   *runtime.Scheme
	Observer PodObserver
	cfg      ControllerConfig
}

type PodObserver interface {
	ObservePods(stackID string, observations []PodObservation)
}

func NewController(k8sClient client.Client, scheme *runtime.Scheme, cfg ControllerConfig) (*Controller, error) {
	cfg = cfg.withDefaults()
	if cfg.StackID == "" {
		return nil, errors.New("stack id is required")
	}
	if k8sClient == nil {
		return nil, errors.New("kubernetes client is nil")
	}
	if scheme == nil {
		return nil, errors.New("scheme is nil")
	}
	return &Controller{Client: k8sClient, Scheme: scheme, cfg: cfg}, nil
}

func (c *Controller) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.ReconcileInterval)
	defer ticker.Stop()

	for {
		if err := c.Reconcile(ctx); err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.cfg.ErrorBackoff):
				continue
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Controller) Reconcile(ctx context.Context) error {
	var stack olricv1alpha1.OlricStack
	if err := c.Get(ctx, types.NamespacedName{Name: c.cfg.StackID, Namespace: c.cfg.Namespace}, &stack); err != nil {
		return err
	}

	resources := []client.Object{
		workloads.OlricHeadlessService(&stack),
		workloads.OlricStatefulSet(&stack),
	}
	for _, obj := range resources {
		if err := controllerutil.SetControllerReference(&stack, obj, c.Scheme); err != nil {
			return err
		}
		if err := c.applyOwned(ctx, obj); err != nil {
			return err
		}
	}
	if err := c.observePods(ctx); err != nil {
		return err
	}
	return nil
}

func (c *Controller) observePods(ctx context.Context) error {
	if c.Observer == nil {
		return nil
	}

	var pods corev1.PodList
	stackRef := &olricv1alpha1.OlricStack{ObjectMeta: metav1.ObjectMeta{Name: c.cfg.StackID, Namespace: c.cfg.Namespace}}
	if err := c.List(ctx, &pods, client.InNamespace(c.cfg.Namespace), client.MatchingLabels(workloads.StackLabels(stackRef, workloads.ComponentOlricNode))); err != nil {
		return err
	}

	now := time.Now()
	observations := make([]PodObservation, 0, len(pods.Items))
	for _, pod := range pods.Items {
		observations = append(observations, PodObservation{
			PodName: pod.Name,
			PodIP:   pod.Status.PodIP,
			Ready:   isPodReady(pod),
			Phase:   string(pod.Status.Phase),
			SeenAt:  now,
		})
	}
	sort.Slice(observations, func(i, j int) bool {
		return observations[i].PodIP < observations[j].PodIP
	})
	c.Observer.ObservePods(c.cfg.StackID, observations)
	return nil
}

func (c *Controller) applyOwned(ctx context.Context, desired client.Object) error {
	key := types.NamespacedName{Name: desired.GetName(), Namespace: desired.GetNamespace()}
	current := desired.DeepCopyObject().(client.Object)
	if err := c.Get(ctx, key, current); err != nil {
		if apierrors.IsNotFound(err) {
			return c.Create(ctx, desired)
		}
		return err
	}

	desired.SetResourceVersion(current.GetResourceVersion())
	switch typed := desired.(type) {
	case *corev1.Service:
		currentSvc := current.(*corev1.Service)
		typed.Spec.ClusterIP = currentSvc.Spec.ClusterIP
		typed.Spec.ClusterIPs = currentSvc.Spec.ClusterIPs
		typed.Spec.IPFamilies = currentSvc.Spec.IPFamilies
		typed.Spec.IPFamilyPolicy = currentSvc.Spec.IPFamilyPolicy
	case *appsv1.StatefulSet:
		currentStatefulSet := current.(*appsv1.StatefulSet)
		typed.Spec.ServiceName = currentStatefulSet.Spec.ServiceName
		typed.Spec.Selector = currentStatefulSet.Spec.Selector
		typed.Spec.VolumeClaimTemplates = currentStatefulSet.Spec.VolumeClaimTemplates
	}
	return c.Update(ctx, desired)
}

func isPodReady(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
