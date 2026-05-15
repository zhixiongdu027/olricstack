package watchdog

import (
	"context"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const epochKey = "topologyEpoch"
const generationKey = "watchdogGeneration"

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
	key := types.NamespacedName{Name: s.name, Namespace: s.namespace}
	for {
		var cm corev1.ConfigMap
		if err := s.client.Get(ctx, key, &cm); err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			cm = corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: s.name, Namespace: s.namespace},
				Data:       map[string]string{epochKey: strconv.FormatInt(epoch, 10)},
			}
			if err := s.client.Create(ctx, &cm); err != nil {
				if apierrors.IsAlreadyExists(err) {
					continue
				}
				return err
			}
			return nil
		}
		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}
		current, err := parseStoredCounter(cm.Data, epochKey)
		if err != nil {
			return err
		}
		if current >= epoch {
			return nil
		}
		cm.Data[epochKey] = strconv.FormatInt(epoch, 10)
		if err := s.client.Update(ctx, &cm); err != nil {
			if apierrors.IsConflict(err) {
				continue
			}
			return err
		}
		return nil
	}
}

func (s *ConfigMapEpochStore) NextGeneration(ctx context.Context, stackID string) (int64, error) {
	for {
		var cm corev1.ConfigMap
		key := types.NamespacedName{Name: s.name, Namespace: s.namespace}
		if err := s.client.Get(ctx, key, &cm); err != nil {
			if !apierrors.IsNotFound(err) {
				return 0, err
			}
			cm = corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: s.name, Namespace: s.namespace},
				Data: map[string]string{
					generationKey: "1",
				},
			}
			if err := s.client.Create(ctx, &cm); err != nil {
				if apierrors.IsAlreadyExists(err) {
					continue
				}
				return 0, err
			}
			return 1, nil
		}
		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}
		current, err := parseStoredCounter(cm.Data, generationKey)
		if err != nil {
			return 0, err
		}
		next := current + 1
		if next <= 0 {
			next = 1
		}
		cm.Data[generationKey] = strconv.FormatInt(next, 10)
		if err := s.client.Update(ctx, &cm); err != nil {
			if apierrors.IsConflict(err) {
				continue
			}
			return 0, err
		}
		return next, nil
	}
}

func parseStoredCounter(data map[string]string, key string) (int64, error) {
	value := data[key]
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}
