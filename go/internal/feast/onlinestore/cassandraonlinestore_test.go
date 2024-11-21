package onlinestore

import (
	"github.com/gocql/gocql"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/stretchr/testify/assert"
)

func TestNewCassandraOnlineStoreDefaults(t *testing.T) {
	var config = map[string]interface{}{}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 4,
	}
	store, err := NewCassandraOnlineStore("test", rc, config)
	assert.Nil(t, err)
	assert.Equal(t, store.hosts, "127.0.0.1")
	assert.Equal(t, store.keyspace, "scylladb")
	assert.Equal(t, store.clusterConfigs.Authenticator, gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	})
	assert.Equal(t, store.clusterConfigs.ProtoVersion, 4)
	assert.Nil(t, store.session)
}

//func TestCassandraOnlineStore_SerializeCassandraEntityKey(t *testing.T) {
//	store := CassandraOnlineStore{}
//	entityKey := &types.EntityKey{
//		JoinKeys:     []string{"key1", "key2"},
//		EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
//	}
//	_, err := store.serializeCassandraEntityKey(entityKey, 2)
//	assert.Nil(t, err)
//}
//
//func TestCassandraOnlineStore_SerializeCassandraEntityKey_InvalidEntityKey(t *testing.T) {
//	store := CassandraOnlineStore{}
//	entityKey := &types.EntityKey{
//		JoinKeys:     []string{"key1"},
//		EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
//	}
//	_, err := store.serializeCassandraEntityKey(entityKey, 2)
//	assert.NotNil(t, err)
//}
//
//func TestCassandraOnlineStore_SerializeValue(t *testing.T) {
//	store := CassandraOnlineStore{}
//	_, _, err := store.serializeValue(&types.Value_StringVal{StringVal: "value1"}, 2)
//	assert.Nil(t, err)
//}
//
//func TestCassandraOnlineStore_SerializeValue_InvalidValue(t *testing.T) {
//	store := CassandraOnlineStore{}
//	_, _, err := store.serializeValue(nil, 2)
//	assert.NotNil(t, err)
//}
//
//func TestCassandraOnlineStore_BuildCassandraKeys(t *testing.T) {
//	store := CassandraOnlineStore{}
//	entityKeys := []*types.EntityKey{
//		{
//			JoinKeys:     []string{"key1", "key2"},
//			EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
//		},
//	}
//	_, _, err := store.buildCassandraKeys(entityKeys)
//	assert.Nil(t, err)
//}
//
//func TestCassandraOnlineStore_BuildCassandraKeys_InvalidEntityKeys(t *testing.T) {
//	store := CassandraOnlineStore{}
//	entityKeys := []*types.EntityKey{
//		{
//			JoinKeys:     []string{"key1"},
//			EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
//		},
//	}
//	_, _, err := store.buildCassandraKeys(entityKeys)
//	assert.NotNil(t, err)
//}
//
//func TestCassandraOnlineStore_OnlineRead_HappyPath(t *testing.T) {
//	store := CassandraOnlineStore{}
//	entityKeys := []*types.EntityKey{
//		{
//			JoinKeys:     []string{"key1", "key2"},
//			EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
//		},
//	}
//	featureViewNames := []string{"featureView1"}
//	featureNames := []string{"feature1"}
//
//	_, err := store.OnlineRead(context.Background(), entityKeys, featureViewNames, featureNames)
//	assert.Nil(t, err)
//}
//
//func TestCassandraOnlineStore_OnlineRead_InvalidEntityKey(t *testing.T) {
//	store := CassandraOnlineStore{}
//	entityKeys := []*types.EntityKey{
//		{
//			JoinKeys:     []string{"key1"},
//			EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
//		},
//	}
//	featureViewNames := []string{"featureView1"}
//	featureNames := []string{"feature1"}
//
//	_, err := store.OnlineRead(context.Background(), entityKeys, featureViewNames, featureNames)
//	assert.NotNil(t, err)
//}
//
//func TestCassandraOnlineStore_OnlineRead_NoFeatureViewNames(t *testing.T) {
//	store := CassandraOnlineStore{}
//	entityKeys := []*types.EntityKey{
//		{
//			JoinKeys:     []string{"key1", "key2"},
//			EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
//		},
//	}
//	featureViewNames := []string{}
//	featureNames := []string{"feature1"}
//
//	_, err := store.OnlineRead(context.Background(), entityKeys, featureViewNames, featureNames)
//	assert.NotNil(t, err)
//}
//
//func TestCassandraOnlineStore_OnlineRead_NoFeatureNames(t *testing.T) {
//	store := CassandraOnlineStore{}
//	entityKeys := []*types.EntityKey{
//		{
//			JoinKeys:     []string{"key1", "key2"},
//			EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "value1"}}, {Val: &types.Value_StringVal{StringVal: "value2"}}},
//		},
//	}
//	featureViewNames := []string{"featureView1"}
//	featureNames := []string{}
//
//	_, err := store.OnlineRead(context.Background(), entityKeys, featureViewNames, featureNames)
//	assert.NotNil(t, err)
//}
