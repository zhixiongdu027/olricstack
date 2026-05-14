package stringkv

import (
	"context"
	"errors"
	"fmt"
	"time"

	olric "github.com/olric-data/olric"
)

type OlricClient interface {
	NewDMap(name string, options ...olric.DMapOption) (olric.DMap, error)
}

type OlricProvider struct {
	client OlricClient
}

func NewOlricProvider(client OlricClient) (*OlricProvider, error) {
	if client == nil {
		return nil, errors.New("olric client is nil")
	}
	return &OlricProvider{client: client}, nil
}

func (p *OlricProvider) DMap(name string) (DMap, error) {
	dmap, err := p.client.NewDMap(name)
	if err != nil {
		return nil, err
	}
	return olricDMap{inner: dmap}, nil
}

type olricDMap struct {
	inner olric.DMap
}

func (d olricDMap) Get(ctx context.Context, key string) (string, error) {
	resp, err := d.inner.Get(ctx, key)
	if err != nil {
		if errors.Is(err, olric.ErrKeyNotFound) {
			return "", ErrNotFound
		}
		return "", err
	}
	value, err := resp.String()
	if err != nil {
		return "", fmt.Errorf("decode olric string value: %w", err)
	}
	return value, nil
}

func (d olricDMap) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	var options []olric.PutOption
	if ttl > 0 {
		options = append(options, olric.PX(ttl))
	}
	return d.inner.Put(ctx, key, value, options...)
}

func (d olricDMap) Delete(ctx context.Context, key string) error {
	_, err := d.inner.Delete(ctx, key)
	if errors.Is(err, olric.ErrKeyNotFound) {
		return ErrNotFound
	}
	return err
}

func (d olricDMap) Expire(ctx context.Context, key string, ttl time.Duration) error {
	err := d.inner.Expire(ctx, key, ttl)
	if errors.Is(err, olric.ErrKeyNotFound) {
		return ErrNotFound
	}
	return err
}

var _ DMapProvider = (*OlricProvider)(nil)
