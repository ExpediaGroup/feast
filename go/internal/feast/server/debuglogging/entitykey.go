package debuglogging

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"google.golang.org/protobuf/proto"
)

// HashEntityKeys returns one salted sha256 hash per join-key present in
// entities, sorted by join-key name for deterministic log output. Entity
// values are never included in plaintext — only the hash.
func HashEntityKeys(entities map[string]*prototypes.RepeatedValue, salt string) []string {
	names := make([]string, 0, len(entities))
	for name := range entities {
		names = append(names, name)
	}
	sort.Strings(names)

	hashes := make([]string, 0, len(names))
	for _, name := range names {
		data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(entities[name])
		if err != nil {
			continue
		}
		sum := sha256.Sum256(append([]byte(salt), data...))
		hashes = append(hashes, "sha256:"+hex.EncodeToString(sum[:]))
	}
	return hashes
}
