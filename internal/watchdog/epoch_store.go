package watchdog

import (
	"context"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const epochKey = "topologyEpoch"

type ConfigMapEpochStore struct {
	client    client.Client
	namespace string
	name      string
}

func NewConfigMapEpochStore(k8sClient client.Client, namespace, name string) *ConfigMapEpochStore {
	return &ConfigMapEpochStore{client: k8sClient, namespace: namespace, name: name}
}

func (s *ConfigMapEpochStore) LoadEpoch(ctx context.Context, stackID string) (int64, error) {
	var cm corev1.ConfigMap
	if err := s.client.Get(ctx, types.NamespacedName{Name: s.name, Namespace: s.namespace}, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return 0, nil
		}
		return 0, err
	}
	if cm.Data == nil {
		return 0, nil
	}
	value := cm.Data[epochKey]
	if value == "" {
		return 0, nil
	}
	return strconv.ParseInt(value, 10, 64)
}

func (s *ConfigMapEpochStore) SaveEpoch(ctx context.Context, stackID string, epoch int64) error {
	var cm corev1.ConfigMap
	key := types.NamespacedName{Name: s.name, Namespace: s.namespace}
	if err := s.client.Get(ctx, key, &cm); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		cm = corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: s.name, Namespace: s.namespace},
			Data:       map[string]string{epochKey: strconv.FormatInt(epoch, 10)},
		}
		return s.client.Create(ctx, &cm)
	}
	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	current, _ := strconv.ParseInt(cm.Data[epochKey], 10, 64)
	if current >= epoch {
		return nil
	}
	cm.Data[epochKey] = strconv.FormatInt(epoch, 10)
	return s.client.Update(ctx, &cm)
}
