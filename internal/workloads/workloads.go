package workloads

import (
	"fmt"

	olricv1alpha1 "github.com/zhixiongdu/olricstack/api/olric/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	LabelStackID   = "olric.io/stack-id"
	LabelComponent = "app.kubernetes.io/component"

	ComponentOlricNode    = "olric-node"
	ComponentOlricSidecar = "olric-sidecar"
	ComponentWatchdog     = "watchdog"

	DefaultNodeImage     = "olricstack/olric-node:latest"
	DefaultSidecarImage  = "olricstack/olric-sidecar:latest"
	DefaultWatchdogImage = "olricstack/watchdog:latest"

	WatchdogPort   = int32(8081)
	OlricPort      = int32(3320)
	RESPPort       = int32(3321)
	MemberlistPort = int32(3322)

	// SharedVolumeName backs the shm ring + control unix socket. It is a
	// tmpfs (emptyDir{Memory}) so producer and consumer share the same
	// pages without disk syscalls. Pod reschedule loses this volume; the
	// "ack = shm append" semantic explicitly accepts that loss.
	SharedVolumeName  = "olric-shared"
	SharedMountPath   = "/var/lib/olricstack/shared"
	SharedMemoryLimit = "256Mi"

	WatchdogClusterRoleName = "olricstack-watchdog"

	AnnotationServiceScope = "olric.io/service-scope"
	ServiceScopeInternal   = "internal-cluster-only"
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

func WatchdogServiceAccountName(stack *olricv1alpha1.OlricStack) string {
	return WatchdogName(stack)
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
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxUnavailable: intstrPtr(1),
					MaxSurge:       intstrPtr(1),
				},
			},
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: WatchdogServiceAccountName(stack),
					Containers: []corev1.Container{{
						Name:  "watchdog",
						Image: valueOrDefault(stack.Spec.WatchdogImage, DefaultWatchdogImage),
						Ports: []corev1.ContainerPort{{
							Name:          "grpc",
							ContainerPort: WatchdogPort,
						}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								GRPC: &corev1.GRPCAction{Port: WatchdogPort},
							},
							PeriodSeconds:    2,
							FailureThreshold: 1,
						},
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

func WatchdogServiceAccount(stack *olricv1alpha1.OlricStack) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      WatchdogServiceAccountName(stack),
			Namespace: stack.Namespace,
			Labels:    StackLabels(stack, ComponentWatchdog),
		},
	}
}

func WatchdogRoleBinding(stack *olricv1alpha1.OlricStack) *rbacv1.RoleBinding {
	labels := StackLabels(stack, ComponentWatchdog)
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      WatchdogServiceAccountName(stack),
			Namespace: stack.Namespace,
			Labels:    labels,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     WatchdogClusterRoleName,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      WatchdogServiceAccountName(stack),
			Namespace: stack.Namespace,
		}},
	}
}

func OlricService(stack *olricv1alpha1.OlricStack) *corev1.Service {
	labels := StackLabels(stack, ComponentOlricNode)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        OlricName(stack),
			Namespace:   stack.Namespace,
			Labels:      labels,
			Annotations: map[string]string{AnnotationServiceScope: ServiceScopeInternal},
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name:       "resp",
				Port:       RESPPort,
				TargetPort: intstr.FromInt32(RESPPort),
			}},
		},
	}
}

// OlricDeployment returns the Deployment that runs olric-node + olric-sidecar
// as two containers in the same Pod, sharing a tmpfs volume for the shm ring
// and the unix-socket control plane.
func OlricDeployment(stack *olricv1alpha1.OlricStack) *appsv1.Deployment {
	labels := StackLabels(stack, ComponentOlricNode)
	replicas := int32(3)
	if stack.Spec.Replicas != nil {
		replicas = *stack.Spec.Replicas
	}

	sharedSize := resource.MustParse(SharedMemoryLimit)
	memoryMedium := corev1.StorageMediumMemory

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      OlricName(stack),
			Namespace: stack.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxUnavailable: intstrPtr(1),
					MaxSurge:       intstrPtr(1),
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{{
						Name: SharedVolumeName,
						VolumeSource: corev1.VolumeSource{
							EmptyDir: &corev1.EmptyDirVolumeSource{
								Medium:    memoryMedium,
								SizeLimit: &sharedSize,
							},
						},
					}},
					Containers: []corev1.Container{
						{
							Name:      "olric-node",
							Image:     valueOrDefault(stack.Spec.Image, DefaultNodeImage),
							Resources: stack.Spec.Resources,
							Ports: []corev1.ContainerPort{{
								Name:          "olric-internal",
								ContainerPort: OlricPort,
							}, {
								Name:          "resp",
								ContainerPort: RESPPort,
							}, {
								Name:          "memberlist",
								ContainerPort: MemberlistPort,
							}},
							VolumeMounts: []corev1.VolumeMount{
								{Name: SharedVolumeName, MountPath: SharedMountPath},
							},
							Env: []corev1.EnvVar{
								{Name: "STACK_ID", Value: stack.Name},
								{Name: "WATCHDOG_SVC_NAME", Value: fmt.Sprintf("%s.%s.svc.cluster.local:%d", WatchdogName(stack), stack.Namespace, WatchdogPort)},
								{Name: "HEARTBEAT_INTERVAL", Value: "10s"},
								{Name: "WATCHDOG_RECONNECT_INTERVAL", Value: "3s"},
								{Name: "OLRIC_BIND_ADDR", Value: "0.0.0.0"},
								{Name: "OLRIC_BIND_PORT", Value: fmt.Sprintf("%d", OlricPort)},
								{Name: "RESP_BIND_ADDR", Value: "0.0.0.0"},
								{Name: "RESP_BIND_PORT", Value: fmt.Sprintf("%d", RESPPort)},
								{Name: "OLRIC_MEMBERLIST_BIND_ADDR", Value: "0.0.0.0"},
								{Name: "OLRIC_MEMBERLIST_BIND_PORT", Value: fmt.Sprintf("%d", MemberlistPort)},
								{Name: "OLRIC_MEMBERLIST_ENV", Value: "lan"},
								{Name: "RING_PATH", Value: SharedMountPath + "/oplog.ring"},
								{Name: "CONTROL_SOCKET", Value: SharedMountPath + "/oplog.sock"},
								{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
								{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
							},
						},
						{
							Name:  "olric-sidecar",
							Image: valueOrDefault(stack.Spec.SidecarImage, DefaultSidecarImage),
							VolumeMounts: []corev1.VolumeMount{{
								Name:      SharedVolumeName,
								MountPath: SharedMountPath,
							}},
							Env: []corev1.EnvVar{
								{Name: "STACK_ID", Value: stack.Name},
								{Name: "MYSQL_DSN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &stack.Spec.MySQLDSNSecret}},
								{Name: "RING_PATH", Value: SharedMountPath + "/oplog.ring"},
								{Name: "CONTROL_SOCKET", Value: SharedMountPath + "/oplog.sock"},
								{Name: "RING_CAPACITY_BYTES", Value: "16777216"},
								{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
							},
						},
					},
				},
			},
		},
	}
}

func intstrPtr(value int) *intstr.IntOrString {
	out := intstr.FromInt(value)
	return &out
}

func valueOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
