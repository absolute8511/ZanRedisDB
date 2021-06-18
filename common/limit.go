package common

import "errors"

var (
	errKeySize    = errors.New("invalid key size")
	errSubKeySize = errors.New("invalid sub key size")
)

const (
	//max key size
	MaxKeySize int = 10240

	// subkey length for hash/set/zset
	MaxSubKeyLen int = 10240

	//max value size
	MaxValueSize int = 1024 * 1024 * 8
)

func CheckKey(key []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return errKeySize
	}
	return nil
}

func CheckSubKey(subkey []byte) error {
	if len(subkey) > MaxSubKeyLen {
		return errSubKeySize
	}
	return nil
}

func CheckKeySubKey(key []byte, field []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return errKeySize
	} else if len(field) > MaxSubKeyLen {
		return errSubKeySize
	}
	return nil
}
