package onlinestore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/utils"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/gocql/gocql"
	"github.com/golang/protobuf/proto"
	"github.com/rs/zerolog/log"

	"google.golang.org/protobuf/types/known/timestamppb"
	gocqltrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/gocql/gocql"
)

type CassandraOnlineStore struct {
	project string

	// Cluster configurations for Cassandra/ScyllaDB
	clusterConfigs *gocqltrace.ClusterConfig

	// Session object that holds information about the connection to the cluster
	session *gocqltrace.Session

	// keyspace of the table. Defaulted to using the project name
	keyspace string

	// Host IP addresses of the cluster
	hosts []string

	config *registry.RepoConfig
}

func NewCassandraOnlineStore(project string, config *registry.RepoConfig, onlineStoreConfig map[string]interface{}) (*CassandraOnlineStore, error) {
	store := CassandraOnlineStore{
		project: project,
		config:  config,
	}

	// Parse host_name and Ips
	cassandraHosts, ok := onlineStoreConfig["hosts"]
	if !ok {
		cassandraHosts = []interface{}{"127.0.0.1"}
		log.Warn().Msg("host not provided: Using 127.0.0.1 instead")
	}

	var rawCassandraHosts []interface{}
	if rawCassandraHosts, ok = cassandraHosts.([]interface{}); !ok {
		return nil, fmt.Errorf("didn't pass a list of hosts in the 'hosts' field")
	}

	var cassandraHostsStr = make([]string, len(rawCassandraHosts))
	for i, rawHost := range rawCassandraHosts {
		hostStr, ok := rawHost.(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert a host to a string: %+v", rawHost)
		}
		cassandraHostsStr[i] = hostStr
	}

	username, ok := onlineStoreConfig["username"]
	if !ok {
		username = "cassandra"
		log.Warn().Msg("username not defined: Using default username instead")
	}

	password, ok := onlineStoreConfig["password"]
	if !ok {
		password = "cassandra"
		log.Warn().Msg("password not defined: Using default password instead")
	}

	var usernameStr string
	if usernameStr, ok = username.(string); !ok {
		return nil, fmt.Errorf("failed to convert username to string: %+v", usernameStr)
	}

	var passwordStr string
	if passwordStr, ok = password.(string); !ok {
		return nil, fmt.Errorf("failed to convert password to string: %+v", passwordStr)
	}

	keyspace, ok := onlineStoreConfig["keyspace"]
	if !ok {
		keyspace = "scylladb"
		log.Warn().Msg("Keyspace not defined: Using 'scylladb' as keyspace instead")
	}
	store.keyspace = keyspace.(string)

	var keyspaceStr string
	if keyspaceStr, ok = keyspace.(string); !ok {
		return nil, fmt.Errorf("failed to convert keyspace to string: %+v", keyspaceStr)
	}

	protocolVersion, ok := onlineStoreConfig["protocol_version"]
	if !ok {
		protocolVersion = 4.0
		log.Warn().Msg("protocol_version not specified: Using 4 instead")
	}
	protocolVersionInt := int(protocolVersion.(float64))

	redisTraceServiceName := os.Getenv("DD_SERVICE") + "-cassandra"
	if redisTraceServiceName == "" {
		redisTraceServiceName = "cassandra.client" // default service name if DD_SERVICE is not set
	}
	store.clusterConfigs = gocqltrace.NewCluster(cassandraHostsStr, gocqltrace.WithServiceName(redisTraceServiceName))
	// TODO: Figure out if we need to offer users the ability to tune the timeouts
	//store.clusterConfigs.ConnectTimeout = 1
	//store.clusterConfigs.Timeout = 1
	store.clusterConfigs.ProtoVersion = protocolVersionInt
	//store.clusterConfigs.Consistency = gocql.Quorum
	store.clusterConfigs.Keyspace = keyspaceStr
	loadBalancingPolicy, ok := onlineStoreConfig["load_balancing"]
	if !ok {
		loadBalancingPolicy = gocql.RoundRobinHostPolicy()
		log.Warn().Msg("No load balancing policy selected; setting Round Robin Host Policy")
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
	}

	store.clusterConfigs.Authenticator = gocql.PasswordAuthenticator{Username: usernameStr, Password: passwordStr}
	createdSession, err := store.clusterConfigs.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("unable to connect to the ScyllaDB database")
	}
	store.session = createdSession
	return &store, nil
}

func (c *CassandraOnlineStore) getFqTableName(tableName string) string {
	return fmt.Sprintf(`"%s"."%s_%s"`, c.keyspace, c.project, tableName)
}

func (c *CassandraOnlineStore) getCQLStatement(tableName string, featureNames []string, nKeys int) string {
	// TODO: Compare with multiple single-key concurrent queries like in the Python feature server
	keyPlaceholders := make([]string, nKeys)
	for i := 0; i < nKeys; i++ {
		keyPlaceholders[i] = "?"
	}

	// this prevents fetching unnecessary features
	quotedFeatureNames := make([]string, len(featureNames))
	for i, featureName := range featureNames {
		quotedFeatureNames[i] = fmt.Sprintf(`'%s'`, featureName)
	}

	return fmt.Sprintf(
		`SELECT "entity_key", "feature_name", "event_ts", "value" FROM %s WHERE "entity_key" IN (%s) AND "feature_name" IN (%s)`,
		tableName,
		strings.Join(keyPlaceholders, ","),
		strings.Join(quotedFeatureNames, ","),
	)
}

func (c *CassandraOnlineStore) buildCassandraEntityKeys(entityKeys []*types.EntityKey) ([]interface{}, map[string]int, error) {
	cassandraKeys := make([]interface{}, len(entityKeys))
	cassandraKeyToEntityIndex := make(map[string]int)
	for i := 0; i < len(entityKeys); i++ {
		var key, err = utils.SerializeEntityKey(entityKeys[i], c.config.EntityKeySerializationVersion)
		if err != nil {
			return nil, nil, err
		}
		// encoding to hex
		encodedKey := hex.EncodeToString(*key)
		cassandraKeys[i] = encodedKey
		cassandraKeyToEntityIndex[encodedKey] = i
	}
	return cassandraKeys, cassandraKeyToEntityIndex, nil
}
func (c *CassandraOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	uniqueNames := make(map[string]int32)
	for _, fvName := range featureViewNames {
		uniqueNames[fvName] = 0
	}
	if len(uniqueNames) != 1 {
		return nil, fmt.Errorf("rejecting OnlineRead as more than 1 feature view was tried to be read at once")
	}

	serializedEntityKeys, serializedEntityKeyToIndex, err := c.buildCassandraEntityKeys(entityKeys)

	if err != nil {
		return nil, fmt.Errorf("error when serializing entity keys for Cassandra")
	}
	results := make([][]FeatureData, len(entityKeys))
	for i := range results {
		results[i] = make([]FeatureData, len(featureNames))
	}

	featureNamesToIdx := make(map[string]int)
	for idx, name := range featureNames {
		featureNamesToIdx[name] = idx
	}

	featureViewName := featureViewNames[0]

	// Prepare the query
	tableName := c.getFqTableName(featureViewName)
	cqlStatement := c.getCQLStatement(tableName, featureNames, len(serializedEntityKeys))
	// Bundle the entity keys in one statement (gocql handles this as concurrent queries)
	scanner := c.session.Query(cqlStatement, serializedEntityKeys...).Iter().Scanner()

	// Process the results
	var entityKey string
	var featureName string
	var eventTs time.Time
	var valueStr []byte
	var deserializedValue types.Value
	for scanner.Next() {
		err := scanner.Scan(&entityKey, &featureName, &eventTs, &valueStr)
		if err != nil {
			return nil, errors.New("could not read row in query for (entity key, feature name, value, event ts)")
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
				Timestamp: timestamppb.Timestamp{Seconds: eventTs.Unix(), Nanos: int32(eventTs.Nanosecond())},
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
		rowIndx := serializedEntityKeyToIndex[entityKey]
		results[rowIndx][featureNamesToIdx[featureName]] = featureValues
	}
	// Check for errors from the Scanner
	if err := scanner.Err(); err != nil {
		return nil, errors.New("failed to scan features: " + err.Error())
	}

	// Will fill feature slots that were left empty with null values
	for i := 0; i < len(entityKeys); i++ {
		for j := 0; j < len(featureNames); j++ {
			if results[i][j].Timestamp.GetSeconds() == 0 {
				results[i][j] = FeatureData{
					Reference: serving.FeatureReferenceV2{
						FeatureViewName: featureViewName,
						FeatureName:     featureViewNames[j],
					},
					Value: types.Value{
						Val: &types.Value_NullVal{
							NullVal: types.Null_NULL,
						},
					},
				}
			}
		}
	}

	return results, nil

}

func (c *CassandraOnlineStore) Destruct() {

}
