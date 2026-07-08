package debuglogging

import (
	"strings"
	"testing"

	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
)

func stringValue(v string) *prototypes.Value {
	return &prototypes.Value{Val: &prototypes.Value_StringVal{StringVal: v}}
}

func TestHashEntityKeys_EmptyMapReturnsEmptySlice(t *testing.T) {
	hashes := HashEntityKeys(map[string]*prototypes.RepeatedValue{}, "salt")
	assert.Empty(t, hashes)
}

func TestHashEntityKeys_OneHashPerJoinKeySortedByName(t *testing.T) {
	entities := map[string]*prototypes.RepeatedValue{
		"customer_id": {Val: []*prototypes.Value{stringValue("c1"), stringValue("c2")}},
		"driver_id":   {Val: []*prototypes.Value{stringValue("d1")}},
	}

	hashes := HashEntityKeys(entities, "salt")

	assert.Len(t, hashes, 2)
	for _, h := range hashes {
		assert.True(t, strings.HasPrefix(h, "sha256:"))
	}

	// Verify the hashes are sorted by key name (customer_id < driver_id alphabetically).
	// Compute the expected hash for each key independently.
	expectedCustomerIDHash := HashEntityKeys(
		map[string]*prototypes.RepeatedValue{
			"customer_id": {Val: []*prototypes.Value{stringValue("c1"), stringValue("c2")}},
		},
		"salt",
	)
	expectedDriverIDHash := HashEntityKeys(
		map[string]*prototypes.RepeatedValue{
			"driver_id": {Val: []*prototypes.Value{stringValue("d1")}},
		},
		"salt",
	)

	assert.Equal(t, expectedCustomerIDHash[0], hashes[0], "first hash should be for customer_id (alphabetically first)")
	assert.Equal(t, expectedDriverIDHash[0], hashes[1], "second hash should be for driver_id (alphabetically second)")
}

func TestHashEntityKeys_NeverContainsPlaintextValue(t *testing.T) {
	entities := map[string]*prototypes.RepeatedValue{
		"customer_id": {Val: []*prototypes.Value{stringValue("super-secret-id-42")}},
	}

	hashes := HashEntityKeys(entities, "salt")

	for _, h := range hashes {
		assert.NotContains(t, h, "super-secret-id-42")
	}
}

func TestHashEntityKeys_DeterministicForSameInputAndSalt(t *testing.T) {
	entities := map[string]*prototypes.RepeatedValue{
		"customer_id": {Val: []*prototypes.Value{stringValue("c1")}},
	}

	first := HashEntityKeys(entities, "salt-a")
	second := HashEntityKeys(entities, "salt-a")

	assert.Equal(t, first, second)
}

func TestHashEntityKeys_DifferentSaltProducesDifferentHash(t *testing.T) {
	entities := map[string]*prototypes.RepeatedValue{
		"customer_id": {Val: []*prototypes.Value{stringValue("c1")}},
	}

	withSaltA := HashEntityKeys(entities, "salt-a")
	withSaltB := HashEntityKeys(entities, "salt-b")

	assert.NotEqual(t, withSaltA, withSaltB)
}
