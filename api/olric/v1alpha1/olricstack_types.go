package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type OlricStackSpec struct {
	Replicas         *int32                      `json:"replicas,omitempty"`
	WatchdogReplicas *int32                      `json:"watchdogReplicas,omitempty"`
	Image            string                      `json:"image,omitempty"`
	SidecarImage     string                      `json:"sidecarImage,omitempty"`
	WatchdogImage    string                      `json:"watchdogImage,omitempty"`
	MySQLDSNSecret   corev1.SecretKeySelector    `json:"mysqlDsnSecret"`
	Resources        corev1.ResourceRequirements `json:"resources,omitempty"`
}

type OlricStackStatus struct {
	ReadyReplicas int32    `json:"readyReplicas,omitempty"`
	PodIPs        []string `json:"podIPs,omitempty"`
	TopologyEpoch int64    `json:"topologyEpoch,omitempty"`
}

type OlricStack struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   OlricStackSpec   `json:"spec,omitempty"`
	Status OlricStackStatus `json:"status,omitempty"`
}

type OlricStackList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []OlricStack `json:"items"`
}

func (in *OlricStack) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(OlricStack)
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Spec.MySQLDSNSecret = *in.Spec.MySQLDSNSecret.DeepCopy()
	out.Spec.Resources = *in.Spec.Resources.DeepCopy()
	out.Status.PodIPs = append([]string(nil), in.Status.PodIPs...)
	return out
}

func (in *OlricStackList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(OlricStackList)
	*out = *in
	out.ListMeta = in.ListMeta
	out.Items = make([]OlricStack, len(in.Items))
	for i := range in.Items {
		item := in.Items[i].DeepCopyObject().(*OlricStack)
		out.Items[i] = *item
	}
	return out
}
