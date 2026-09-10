package identity

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

const idBytes = 16

func NewExecutionID() (string, error) {
	return newID("exec")
}

func NewRequestID() (string, error) {
	return newID("req")
}

func newID(prefix string) (string, error) {
	b := make([]byte, idBytes)

	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate %s ID: %w", prefix, err)
	}

	return prefix + "_" + hex.EncodeToString(b), nil
}
