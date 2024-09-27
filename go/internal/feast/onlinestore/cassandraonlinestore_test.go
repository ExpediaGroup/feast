package onlinestore

import (
	"context"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/types"

	"github.com/stretchr/testify/assert"
)

func TestNewCassandraOnlineStore(t *testing.T) {
	var config = map[string]interface{}{
		"username":       "",
		"password":       "",
		"hosts":          "",
		"keyspace":       "",
		"load_balancing": "",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 2,
	}
	store, err := NewCassandraOnlineStore("test", rc, config)
	assert.Nil(t, err)
	assert.Equal(t, store.hosts, "127.0.0.1")
	assert.Equal(t, store.keyspace, "test")
	assert.Equal(t, store.username, "scylla")
	assert.Equal(t, store.password, "scylla")
	assert.Nil(t, store.session)
}

func TestNewCassandraOnlineStoreWithPassword(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 2,
	}
	store, err := NewRedisOnlineStore("test", rc, config)
	assert.Nil(t, err)
	var opts = store.client.Options()
	assert.Equal(t, opts.Addr, "redis://localhost:6379")
	assert.Equal(t, opts.Password, "secret")
}

func TestCassandraOnlineStore_SerializeCassandraEntityKey(t *testing.T) {
	store := CassandraOnlineStore{}
	entityKey := &types.EntityKey{
		JoinKeys:     []string{"key1", "key2"},
		EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
	}
	_, err := store.serializeCassandraEntityKey(entityKey, 2)
	assert.Nil(t, err)
}

func TestCassandraOnlineStore_SerializeCassandraEntityKey_InvalidEntityKey(t *testing.T) {
	store := CassandraOnlineStore{}
	entityKey := &types.EntityKey{
		JoinKeys:     []string{"key1"},
		EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
	}
	_, err := store.serializeCassandraEntityKey(entityKey, 2)
	assert.NotNil(t, err)
}

func TestCassandraOnlineStore_SerializeValue(t *testing.T) {
	store := CassandraOnlineStore{}
	_, _, err := store.serializeValue(&types.Value_StringVal{StringVal: "value1"}, 2)
	assert.Nil(t, err)
}

func TestCassandraOnlineStore_SerializeValue_InvalidValue(t *testing.T) {
	store := CassandraOnlineStore{}
	_, _, err := store.serializeValue(nil, 2)
	assert.NotNil(t, err)
}

func TestCassandraOnlineStore_BuildCassandraKeys(t *testing.T) {
	store := CassandraOnlineStore{}
	entityKeys := []*types.EntityKey{
		{
			JoinKeys:     []string{"key1", "key2"},
			EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
		},
	}
	_, _, err := store.buildCassandraKeys(entityKeys)
	assert.Nil(t, err)
}

func TestCassandraOnlineStore_BuildCassandraKeys_InvalidEntityKeys(t *testing.T) {
	store := CassandraOnlineStore{}
	entityKeys := []*types.EntityKey{
		{
			JoinKeys:     []string{"key1"},
			EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
		},
	}
	_, _, err := store.buildCassandraKeys(entityKeys)
	assert.NotNil(t, err)
}

func TestCassandraOnlineStore_OnlineRead_HappyPath(t *testing.T) {
	store := CassandraOnlineStore{}
	entityKeys := []*types.EntityKey{
		{
			JoinKeys:     []string{"key1", "key2"},
			EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
		},
	}
	featureViewNames := []string{"featureView1"}
	featureNames := []string{"feature1"}

	_, err := store.OnlineRead(context.Background(), entityKeys, featureViewNames, featureNames)
	assert.Nil(t, err)
}

func TestCassandraOnlineStore_OnlineRead_InvalidEntityKey(t *testing.T) {
	store := CassandraOnlineStore{}
	entityKeys := []*types.EntityKey{
		{
			JoinKeys:     []string{"key1"},
			EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
		},
	}
	featureViewNames := []string{"featureView1"}
	featureNames := []string{"feature1"}

	_, err := store.OnlineRead(context.Background(), entityKeys, featureViewNames, featureNames)
	assert.NotNil(t, err)
}

func TestCassandraOnlineStore_OnlineRead_NoFeatureViewNames(t *testing.T) {
	store := CassandraOnlineStore{}
	entityKeys := []*types.EntityKey{
		{
			JoinKeys:     []string{"key1", "key2"},
			EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
		},
	}
	featureViewNames := []string{}
	featureNames := []string{"feature1"}

	_, err := store.OnlineRead(context.Background(), entityKeys, featureViewNames, featureNames)
	assert.NotNil(t, err)
}

func TestCassandraOnlineStore_OnlineRead_NoFeatureNames(t *testing.T) {
	store := CassandraOnlineStore{}
	entityKeys := []*types.EntityKey{
		{
			JoinKeys:     []string{"key1", "key2"},
			EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
		},
	}
	featureViewNames := []string{"featureView1"}
	featureNames := []string{}

	_, err := store.OnlineRead(context.Background(), entityKeys, featureViewNames, featureNames)
	assert.NotNil(t, err)
}
