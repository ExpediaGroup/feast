package onlinestore

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/utils"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/gocql/gocql"
	"github.com/golang/protobuf/proto"
	"github.com/rs/zerolog/log"
	_ "google.golang.org/protobuf/types/known/timestamppb"
	_ "net"
	"sort"
	"time"
	_ "time"
)

type CassandraOnlineStore struct {
	project string

	// Cluster configurations for Cassandra/ScyllaDB
	clusterConfigs *gocql.ClusterConfig

	// Session object that holds information about the connection to the cluster
	session *gocql.Session

	// keyspace of the table. Defaulted to using the project name
	keyspace string

	// Host IP addresses of the cluster
	hostIps []string

	config *registry.RepoConfig
}

func NewCassandraOnlineStore(project string, config *registry.RepoConfig, onlineStoreConfig map[string]interface{}) (*CassandraOnlineStore, error) {
	store := CassandraOnlineStore{
		project: project,
		config:  config,
	}

	var username string
	var password string
	// Parse host_name and Ips
	cassandraHostNames, ok1 := onlineStoreConfig["host_names"]
	cassandraHostIps, ok2 := onlineStoreConfig["hosts"]
	if !ok1 && !ok2 {
		cassandraHostIps = "127.0.0.1"

	}
	var cassandraHostNameStr string
	if cassandraHostNameStr, ok1 = cassandraHostNames.(string); !ok1 {
		return nil, fmt.Errorf("failed to convert host_names to string: %+v", cassandraHostNameStr)
	}

	var cassandraHostIpsStr string
	if cassandraHostIpsStr, ok2 = cassandraHostIps.(string); !ok2 {
		return nil, fmt.Errorf("failed to convert hosts(ip addresses) to string: %+v", cassandraHostIpsStr)
	}

	username = onlineStoreConfig["username"].(string)
	password = onlineStoreConfig["password"].(string)

	if len(username) == 0 {
		username = "scylla"
		log.Warn().Msg("Username not defined: Using default username instead")
	}
	if len(username) == 0 {
		password = "scylla"
		log.Warn().Msg("Password not defined: Using default password instead")
	}

	keyspace, ok := onlineStoreConfig["keyspace"]
	if !ok {
		keyspace = project
		log.Warn().Msgf("Keyspace not defined: Using project name %s as keyspace instead", project)
	}
	var keyspaceStr string
	if keyspaceStr, ok = keyspace.(string); !ok {
		return nil, fmt.Errorf("failed to convert keyspace to string: %+v", keyspaceStr)
	}

	// If you're using host_names, it means that you will need a host resolver to get the IP of the cluster the DB is in
	if len(cassandraHostNameStr) > 0 {
		ec2Instance := utils.NewEC2Instance(cassandraHostNameStr, "us-west-2")
		hostIps, err := ec2Instance.ResolveHostNameToIp()
		if err != nil {
			return nil, fmt.Errorf("Unable to resolve host name %+v to ip address: ", cassandraHostNameStr)
		}
		store.hostIps = hostIps
	} else {
		store.hostIps = []string{cassandraHostNameStr}
	}

	store.clusterConfigs = gocql.NewCluster(store.hostIps...)
	// TODO: Figure out if we need to offer users the ability to tune the timeouts
	//store.clusterConfigs.ConnectTimeout = 1
	//store.clusterConfigs.Timeout = 1
	store.clusterConfigs.ProtoVersion = onlineStoreConfig["protocol"].(int)
	//store.clusterConfigs.Consistency = gocql.Quorum
	store.clusterConfigs.Keyspace = keyspaceStr
	loadBalancingPolicy, ok := onlineStoreConfig["load_balancing"]
	if !ok {
		return nil, nil
	}
	loadBalancingPolicyStr, _ := loadBalancingPolicy.(string)
	if loadBalancingPolicyStr == "DCAwareRoundRobinPolicy" {
		store.clusterConfigs.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
	} else if loadBalancingPolicyStr == "TokenAwarePolicy(DCAwareRoundRobinPolicy)" {
		// Configure fallback policy if unable to reach the shard
		fallback := gocql.RoundRobinHostPolicy()
		// If using ScyllaDB and setting this policy, this makes the driver shard aware to improve performance
		store.clusterConfigs.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)
		if config.OnlineStore["type"] == "scylladb" {
			store.clusterConfigs.Port = 19042
		} else {
			store.clusterConfigs.Port = 9042
		}
	} else {
		return nil, fmt.Errorf("No load balancing policy specified")
	}

	store.clusterConfigs.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}
	createdSession, err1 := gocql.NewSession(*store.clusterConfigs)
	if err1 != nil {
		return nil, fmt.Errorf("Unable to connect to ScyllaDB")
	}
	store.session = createdSession
	return &store, nil
}

func (c *CassandraOnlineStore) getTableName(tableName string) string {
	return fmt.Sprintf(`"%s"_"%s"_""%s"`, c.keyspace, c.project, tableName)
}
func (c *CassandraOnlineStore) getCQLStatement(tableName string) string {
	selectStatement := "SELECT entity_key, feature_name, value, event_ts FROM %s WHERE entity_key IN ?;"
	return fmt.Sprintf(selectStatement, tableName)

}

func (c *CassandraOnlineStore) buildCassandraEntityKeys(entityKeys []*types.EntityKey) ([]interface{}, map[string]int, error) {
	cassandraKeys := make([]interface{}, len(entityKeys))
	cassandraKeyToEntityIndex := make(map[string]int)
	for i := 0; i < len(entityKeys); i++ {
		var key, err = serializeCassandraEntityKey(entityKeys[i], c.config.EntityKeySerializationVersion)
		if err != nil {
			return nil, nil, err
		}
		// encoding to hex
		cassandraKeys[i] = hex.EncodeToString(*key)
		cassandraKeyToEntityIndex[hashSerializedEntityKey(key)] = i
	}
	return cassandraKeys, cassandraKeyToEntityIndex, nil
}
func (c *CassandraOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {

	serializedEntityKeys, serializedEntityKeyToIndex, err := c.buildCassandraEntityKeys(entityKeys)
	if err != nil {
		return nil, fmt.Errorf("Error when serializing entity keys for Cassandra")
	}
	results := make([][]FeatureData, len(entityKeys))
	featureNamesToIdx := make(map[string]int)
	for idx, name := range featureNames {
		featureNamesToIdx[name] = idx
	}

	for _, featureViewName := range featureViewNames {
		// Prepare the query
		tableName := c.getTableName(featureViewName)
		cqlStatement := c.getCQLStatement(tableName)
		// Bundle the entity keys in one statement (gocql handles this as concurrent queries)
		query := c.session.Query(cqlStatement, serializedEntityKeys...)
		scan := query.Iter().Scanner()

		// Process the results
		var entityKey []byte
		var featureName string
		var eventTs time.Time
		var valueStr []byte
		var deserializedValue types.Value
		for scan.Next() {
			err := scan.Scan(&entityKey, &featureName, &valueStr, &eventTs)
			if err != nil {
				return nil, errors.New("Could not read row in query for (entity key, feature name, value, event ts)")
			}
			if err := proto.Unmarshal(valueStr, &deserializedValue); err != nil {
				return nil, errors.New("error converting parsed Cassandra Value to types.Value")
			}

			var featureValues FeatureData
			if deserializedValue.Val != nil {
				// Convert the value to a FeatureData struct
				featureValues = FeatureData{
					Reference: serving.FeatureReferenceV2{
						FeatureViewName: featureViewName,
						FeatureName:     featureName,
					},
					Value: types.Value{
						Val: deserializedValue.Val,
					},
				}
			} else {
				// Return FeatureData with a null value
				featureValues = FeatureData{
					Reference: serving.FeatureReferenceV2{
						FeatureViewName: featureViewName,
						FeatureName:     featureName,
					},
					Value: types.Value{
						Val: &types.Value_NullVal{
							NullVal: types.Null_NULL,
						},
					},
				}

			}
			// Add the FeatureData to the results
			rowIndx := serializedEntityKeyToIndex[hashSerializedEntityKey(&entityKey)]
			results[rowIndx][featureNamesToIdx[featureName]] = featureValues
		}
		// Check for errors from the Scanner
		if err := scan.Err(); err != nil {
			return nil, err
		}
	}

	return results, nil

}

// Serialize entity key to a bytestring so that it can be used as a lookup key in a hash table.
func serializeCassandraEntityKey(entityKey *types.EntityKey, entityKeySerializationVersion int64) (*[]byte, error) {
	// Ensure that we have the right amount of join keys and entity values
	if len(entityKey.JoinKeys) != len(entityKey.EntityValues) {
		return nil, fmt.Errorf("the amount of join key names and entity values don't match: %s vs %s", entityKey.JoinKeys, entityKey.EntityValues)
	}
	// Make sure that join keys are sorted so that we have consistent key building
	m := make(map[string]*types.Value)

	for i := 0; i < len(entityKey.JoinKeys); i++ {
		m[entityKey.JoinKeys[i]] = entityKey.EntityValues[i]
	}

	keys := make([]string, 0, len(m))
	for k := range entityKey.JoinKeys {
		keys = append(keys, entityKey.JoinKeys[k])
	}
	sort.Strings(keys)

	// Build the key
	length := 5 * len(keys)
	bufferList := make([][]byte, length)

	for i := 0; i < len(keys); i++ {
		offset := i * 2
		byteBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(byteBuffer, uint32(types.ValueType_Enum_value["STRING"]))
		bufferList[offset] = byteBuffer
		bufferList[offset+1] = []byte(keys[i])
	}

	for i := 0; i < len(keys); i++ {
		offset := (2 * len(keys)) + (i * 3)
		value := m[keys[i]].GetVal()

		valueBytes, valueTypeBytes, err := serializeCassandraValue(value, entityKeySerializationVersion)
		if err != nil {
			return valueBytes, err
		}

		typeBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(typeBuffer, uint32(valueTypeBytes))

		lenBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuffer, uint32(len(*valueBytes)))

		bufferList[offset+0] = typeBuffer
		bufferList[offset+1] = lenBuffer
		bufferList[offset+2] = *valueBytes
	}

	// Convert from an array of byte arrays to a single byte array
	var entityKeyBuffer []byte
	for i := 0; i < len(bufferList); i++ {
		entityKeyBuffer = append(entityKeyBuffer, bufferList[i]...)
	}

	return &entityKeyBuffer, nil
}

func serializeCassandraValue(value interface{}, entityKeySerializationVersion int64) (*[]byte, types.ValueType_Enum, error) {
	// TODO: Implement support for other types (at least the major types like ints, strings, bytes)
	switch x := (value).(type) {
	case *types.Value_StringVal:
		valueString := []byte(x.StringVal)
		return &valueString, types.ValueType_STRING, nil
	case *types.Value_BytesVal:
		return &x.BytesVal, types.ValueType_BYTES, nil
	case *types.Value_Int32Val:
		valueBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(valueBuffer, uint32(x.Int32Val))
		return &valueBuffer, types.ValueType_INT32, nil
	case *types.Value_Int64Val:
		if entityKeySerializationVersion <= 1 {
			//  We unfortunately have to use 32 bit here for backward compatibility :(
			valueBuffer := make([]byte, 4)
			binary.LittleEndian.PutUint32(valueBuffer, uint32(x.Int64Val))
			return &valueBuffer, types.ValueType_INT64, nil
		} else {
			valueBuffer := make([]byte, 8)
			binary.LittleEndian.PutUint64(valueBuffer, uint64(x.Int64Val))
			return &valueBuffer, types.ValueType_INT64, nil
		}
	case nil:
		return nil, types.ValueType_INVALID, fmt.Errorf("could not detect type for %v", x)
	default:
		return nil, types.ValueType_INVALID, fmt.Errorf("could not detect type for %v", x)
	}
}

func hashSerializedEntityKey(serializedEntityKey *[]byte) string {
	if serializedEntityKey == nil {
		return ""
	}
	h := sha1.New()
	h.Write(*serializedEntityKey)
	sha1_hash := hex.EncodeToString(h.Sum(nil))
	return sha1_hash
}

func (c *CassandraOnlineStore) Destruct() {

}
