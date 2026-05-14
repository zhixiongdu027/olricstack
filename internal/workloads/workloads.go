package workloads

import (
	"fmt"

	olricv1alpha1 "github.com/zhixiongdu/olricstack/api/olric/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	LabelStackID   = "olric.io/stack-id"
	LabelComponent = "app.kubernetes.io/component"

	ComponentOlricNode = "olric-node"
	ComponentWatchdog  = "watchdog"

	DefaultNodeImage     = "olricstack/olric-node:latest"
	DefaultWatchdogImage = "olricstack/watchdog:latest"

	WatchdogPort = int32(8081)

	OlricDataVolumeName = "olric-data"
	OlricDataMountPath  = "/var/lib/olricstack"
)

func StackLabels(stack *olricv1alpha1.OlricStack, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     "olricstack",
		"app.kubernetes.io/instance": stack.Name,
		LabelStackID:                 stack.Name,
		LabelComponent:               component,
	}
}

func WatchdogName(stack *olricv1alpha1.OlricStack) string {
	return stack.Name + "-watchdog"
}

func OlricName(stack *olricv1alpha1.OlricStack) string {
	return stack.Name + "-olric"
}

func WatchdogService(stack *olricv1alpha1.OlricStack) *corev1.Service {
	labels := StackLabels(stack, ComponentWatchdog)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      WatchdogName(stack),
			Namespace: stack.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name:       "grpc",
				Port:       WatchdogPort,
				TargetPort: intstr.FromInt32(WatchdogPort),
			}},
		},
	}
}

func WatchdogDeployment(stack *olricv1alpha1.OlricStack) *appsv1.Deployment {
	labels := StackLabels(stack, ComponentWatchdog)
	replicas := int32(2)
	if stack.Spec.WatchdogReplicas != nil {
		replicas = *stack.Spec.WatchdogReplicas
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      WatchdogName(stack),
			Namespace: stack.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: "olricstack-watchdog",
					Containers: []corev1.Container{{
						Name:  "watchdog",
						Image: valueOrDefault(stack.Spec.WatchdogImage, DefaultWatchdogImage),
						Ports: []corev1.ContainerPort{{
							Name:          "grpc",
							ContainerPort: WatchdogPort,
						}},
						Env: []corev1.EnvVar{
							{Name: "WATCHDOG_ADDR", Value: fmt.Sprintf(":%d", WatchdogPort)},
							{Name: "STACK_ID", Value: stack.Name},
							{Name: "STACK_NAMESPACE", Value: stack.Namespace},
							{Name: "SUSPECT_AFTER", Value: "20s"},
							{Name: "EXPIRE_AFTER", Value: "30s"},
							{Name: "REAP_INTERVAL", Value: "15s"},
							{Name: "BOOKWORM_INTERVAL", Value: "10s"},
							{Name: "TOPOLOGY_LEASE_TTL", Value: "30s"},
							{Name: "WATCHDOG_ROLE", Value: "primary"},
							{Name: "WATCHDOG_RECONCILE_INTERVAL", Value: "10s"},
						},
					}},
				},
			},
		},
	}
}

func OlricHeadlessService(stack *olricv1alpha1.OlricStack) *corev1.Service {
	labels := StackLabels(stack, ComponentOlricNode)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      OlricName(stack),
			Namespace: stack.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels,
			Ports: []corev1.ServicePort{{
				Name: "olric",
				Port: 3320,
			}},
		},
	}
}

func OlricStatefulSet(stack *olricv1alpha1.OlricStack) *appsv1.StatefulSet {
	labels := StackLabels(stack, ComponentOlricNode)
	replicas := int32(3)
	if stack.Spec.Replicas != nil {
		replicas = *stack.Spec.Replicas
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      OlricName(stack),
			Namespace: stack.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: OlricName(stack),
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:      "olric-node",
						Image:     valueOrDefault(stack.Spec.Image, DefaultNodeImage),
						Resources: stack.Spec.Resources,
						VolumeMounts: []corev1.VolumeMount{{
							Name:      OlricDataVolumeName,
							MountPath: OlricDataMountPath,
						}},
						Env: []corev1.EnvVar{
							{Name: "STACK_ID", Value: stack.Name},
							{Name: "WATCHDOG_SVC_NAME", Value: fmt.Sprintf("%s.%s.svc.cluster.local:%d", WatchdogName(stack), stack.Namespace, WatchdogPort)},
							{Name: "HEARTBEAT_INTERVAL", Value: "10s"},
							{Name: "WATCHDOG_RECONNECT_INTERVAL", Value: "3s"},
							{Name: "DIRTY_QUEUE_SIZE", Value: "1024"},
							{Name: "FLUSH_INTERVAL", Value: "1s"},
							{Name: "FLUSH_BATCH_SIZE", Value: "256"},
							{Name: "WAL_PATH", Value: "/var/lib/olricstack/cache.wal"},
							{Name: "MYSQL_DSN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &stack.Spec.MySQLDSNSecret}},
							{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
							{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: OlricDataVolumeName},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resourceQuantity("1Gi"),
						},
					},
				},
			}},
		},
	}
}

func resourceQuantity(value string) resource.Quantity {
	return resource.MustParse(value)
}

func valueOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
