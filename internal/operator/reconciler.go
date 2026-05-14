package operator

import (
	"context"
	"sort"

	olricv1alpha1 "github.com/zhixiongdu/olricstack/api/olric/v1alpha1"
	"github.com/zhixiongdu/olricstack/internal/workloads"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const LabelStackID = workloads.LabelStackID

type OlricStackReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *OlricStackReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var stack olricv1alpha1.OlricStack
	if err := r.Get(ctx, req.NamespacedName, &stack); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	resources := []client.Object{
		workloads.WatchdogService(&stack),
		workloads.WatchdogDeployment(&stack),
	}

	for _, obj := range resources {
		if err := controllerutil.SetControllerReference(&stack, obj, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.applyOwned(ctx, obj); err != nil {
			return ctrl.Result{}, err
		}
	}

	if err := r.updateStatus(ctx, &stack); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *OlricStackReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&olricv1alpha1.OlricStack{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Complete(r)
}

func (r *OlricStackReconciler) applyOwned(ctx context.Context, desired client.Object) error {
	key := types.NamespacedName{Name: desired.GetName(), Namespace: desired.GetNamespace()}
	current := desired.DeepCopyObject().(client.Object)
	if err := r.Get(ctx, key, current); err != nil {
		if apierrors.IsNotFound(err) {
			return r.Create(ctx, desired)
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
	}
	return r.Update(ctx, desired)
}

func (r *OlricStackReconciler) updateStatus(ctx context.Context, stack *olricv1alpha1.OlricStack) error {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(stack.Namespace), client.MatchingLabels(workloads.StackLabels(stack, workloads.ComponentOlricNode))); err != nil {
		return err
	}

	var ready int32
	podIPs := make([]string, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.Status.PodIP != "" {
			podIPs = append(podIPs, pod.Status.PodIP)
		}
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				ready++
				break
			}
		}
	}
	sort.Strings(podIPs)

	stack.Status.ReadyReplicas = ready
	stack.Status.PodIPs = podIPs
	return r.Status().Update(ctx, stack)
}
