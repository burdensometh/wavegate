package v1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
)

const MaxBody = 1 << 20

func Decode(reader io.Reader, destination any) error {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("read v1 JSON: %w", err)
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("read v1 JSON: expected exactly one document")
	}
	return nil
}

func Digest(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func Copy[T any](value T) (T, error) {
	var result T
	data, err := json.Marshal(value)
	if err != nil {
		return result, err
	}
	err = json.Unmarshal(data, &result)
	return result, err
}
