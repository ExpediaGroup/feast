package utils

import (
	"crypto/sha1"
	"encoding/hex"
)

func HashSerializedEntityKey(serializedEntityKey *[]byte) string {
	if serializedEntityKey == nil {
		return ""
	}
	h := sha1.New()
	h.Write(*serializedEntityKey)
	return hex.EncodeToString(h.Sum(nil))
}
