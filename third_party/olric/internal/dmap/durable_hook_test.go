package dmap

import (
	"context"
	"sync"
	"testing"

	"github.com/olric-data/olric/config"
	"github.com/olric-data/olric/internal/cluster/partitions"
	"github.com/olric-data/olric/internal/testcluster"
	"github.com/olric-data/olric/internal/testutil"
	"github.com/olric-data/olric/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDurableHookCalledOnlyForOwnerBusinessWrites(t *testing.T) {
	hook := &recordingDurableHook{}
	cfg := testutil.NewConfig()
	cfg.DurableHook = hook
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(testcluster.NewEnvironment(cfg)).(*Service)
	defer cluster.Shutdown()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(context.Background(), "key", "value", nil)
	require.NoError(t, err)
	hook.requireOrigins(t, "client_set")

	entry := dm.engine.NewEntry()
	entry.SetKey("key")
	entry.SetValue([]byte("repair"))
	entry.SetTimestamp(2)
	env := newEnv(context.Background())
	env.hkey = partitions.HKey(dm.name, "key")
	env.fragment = mustPrimaryFragment(t, dm, env.hkey)
	err = dm.putEntryOnFragment(env, entry)
	require.NoError(t, err)

	env.value = entry.Encode()
	err = dm.putOnReplicaFragment(env)
	require.NoError(t, err)
	hook.requireOrigins(t, "client_set")
}

func TestDurableHookDeleteTombstoneOnOwnerMiss(t *testing.T) {
	hook := &recordingDurableHook{}
	cfg := testutil.NewConfig()
	cfg.DurableHook = hook
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(testcluster.NewEnvironment(cfg)).(*Service)
	defer cluster.Shutdown()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	_, err = dm.Delete(context.Background(), "missing")
	require.NoError(t, err)
	hook.requireOrigins(t, "client_delete")
}

func TestDurableHookExpireOnlyAfterOwnerKeyExists(t *testing.T) {
	hook := &recordingDurableHook{}
	cfg := testutil.NewConfig()
	cfg.DurableHook = hook
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(testcluster.NewEnvironment(cfg)).(*Service)
	defer cluster.Shutdown()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Expire(context.Background(), "missing", 1)
	require.ErrorIs(t, err, ErrKeyNotFound)
	hook.requireOrigins(t)

	err = dm.Put(context.Background(), "key", "value", nil)
	require.NoError(t, err)
	err = dm.Expire(context.Background(), "key", 1)
	require.NoError(t, err)
	hook.requireOrigins(t, "client_set", "client_expire")
}

func TestDurableHookLoadMissReturnsDMapMiss(t *testing.T) {
	hook := &recordingDurableHook{loadErr: storage.ErrKeyNotFound}
	cfg := testutil.NewConfig()
	cfg.DurableHook = hook
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(testcluster.NewEnvironment(cfg)).(*Service)
	defer cluster.Shutdown()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	_, err = dm.Get(context.Background(), "missing")
	require.ErrorIs(t, err, ErrKeyNotFound)
	hook.requireOrigins(t, "client_get_miss")
}

func TestDurableHookFollowsOwnerWhenIngressIsNotOwner(t *testing.T) {
	ingressHook := &recordingDurableHook{}
	ownerHook := &recordingDurableHook{}
	ingressCfg := testutil.NewConfig()
	ingressCfg.DurableHook = ingressHook
	ownerCfg := testutil.NewConfig()
	ownerCfg.DurableHook = ownerHook
	cluster := testcluster.New(NewService)
	ingress := cluster.AddMember(testcluster.NewEnvironment(ingressCfg)).(*Service)
	owner := cluster.AddMember(testcluster.NewEnvironment(ownerCfg)).(*Service)
	defer cluster.Shutdown()

	dm, err := ingress.NewDMap("mydmap")
	require.NoError(t, err)
	key := firstKeyOwnedBy(t, ingress, owner, dm.name)

	err = dm.Put(context.Background(), key, "value", nil)
	require.NoError(t, err)
	ingressHook.requireOrigins(t)
	ownerHook.requireOrigins(t, "client_set")

	err = dm.Expire(context.Background(), key, 1)
	require.NoError(t, err)
	ingressHook.requireOrigins(t)
	ownerHook.requireOrigins(t, "client_set", "client_expire")

	_, err = dm.Delete(context.Background(), key)
	require.NoError(t, err)
	ingressHook.requireOrigins(t)
	ownerHook.requireOrigins(t, "client_set", "client_expire", "client_delete")
}

type recordingDurableHook struct {
	mu      sync.Mutex
	origins []string
	loadErr error
}

func (h *recordingDurableHook) BeforeSet(ctx context.Context, op config.DurableOperation) error {
	h.record(op.Origin)
	return nil
}

func (h *recordingDurableHook) BeforeDelete(ctx context.Context, op config.DurableOperation) error {
	h.record(op.Origin)
	return nil
}

func (h *recordingDurableHook) BeforeExpire(ctx context.Context, op config.DurableOperation) error {
	h.record(op.Origin)
	return nil
}

func (h *recordingDurableHook) LoadOnMiss(ctx context.Context, op config.DurableOperation) (storage.Entry, error) {
	h.record(op.Origin)
	if h.loadErr != nil {
		return nil, h.loadErr
	}
	return nil, ErrKeyNotFound
}

func (h *recordingDurableHook) record(origin string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.origins = append(h.origins, origin)
}

func (h *recordingDurableHook) requireOrigins(t *testing.T, origins ...string) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	require.Equal(t, origins, h.origins)
}

func mustPrimaryFragment(t *testing.T, dm *DMap, hkey uint64) *fragment {
	t.Helper()
	part := dm.getPartitionByHKey(hkey, partitions.PRIMARY)
	f, err := dm.loadOrCreateFragment(part)
	require.NoError(t, err)
	return f
}

func firstKeyOwnedBy(t *testing.T, ingress, owner *Service, dmap string) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		key := testutil.ToKey(i)
		hkey := partitions.HKey(dmap, key)
		member := ingress.primary.PartitionByHKey(hkey).Owner()
		if !member.CompareByName(ingress.rt.This()) && member.CompareByName(owner.rt.This()) {
			return key
		}
	}
	t.Fatal("could not find a key owned by the target owner")
	return ""
}
