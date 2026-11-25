package v1

import "encoding/hex"

func ValidDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
