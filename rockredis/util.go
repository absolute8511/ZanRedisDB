package rockredis

import (
	"encoding/binary"
	"errors"
	"strconv"
)

var errIntNumber = errors.New("invalid integer")

func Int64(v []byte, err error) (int64, error) {
	if err != nil {
		return 0, err
	} else if v == nil || len(v) == 0 {
		return 0, nil
	} else if len(v) != 8 {
		return 0, errIntNumber
	}

	return int64(binary.BigEndian.Uint64(v)), nil
}

func Uint64(v []byte, err error) (uint64, error) {
	if err != nil {
		return 0, err
	} else if v == nil || len(v) == 0 {
		return 0, nil
	} else if len(v) != 8 {
		return 0, errIntNumber
	}

	return binary.BigEndian.Uint64(v), nil
}

func FormatInt64ToSlice(v int64) []byte {
	return strconv.AppendInt(nil, int64(v), 10)
}

func PutInt64(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func StrInt64(v []byte, err error) (int64, error) {
	if err != nil {
		return 0, err
	} else if v == nil {
		return 0, nil
	} else {
		return strconv.ParseInt(string(v), 10, 64)
	}
}

func StrUint64(v []byte, err error) (uint64, error) {
	if err != nil {
		return 0, err
	} else if v == nil {
		return 0, nil
	} else {
		return strconv.ParseUint(string(v), 10, 64)
	}
}

func StrInt32(v []byte, err error) (int32, error) {
	if err != nil {
		return 0, err
	} else if v == nil {
		return 0, nil
	} else {
		res, err := strconv.ParseInt(string(v), 10, 32)
		return int32(res), err
	}
}

func StrInt8(v []byte, err error) (int8, error) {
	if err != nil {
		return 0, err
	} else if v == nil {
		return 0, nil
	} else {
		res, err := strconv.ParseInt(string(v), 10, 8)
		return int8(res), err
	}
}

func MinUint(a uint, b uint) uint {
	if a > b {
		return b
	} else {
		return a
	}
}

func MaxUint(a uint, b uint) uint {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinInt(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func MaxInt(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinUint8(a uint8, b uint8) uint8 {
	if a > b {
		return b
	} else {
		return a
	}
}

func MaxUint8(a uint8, b uint8) uint8 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinInt8(a int8, b int8) int8 {
	if a > b {
		return b
	} else {
		return a
	}
}

func MaxInt8(a int8, b int8) int8 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinUint16(a uint16, b uint16) uint16 {
	if a > b {
		return b
	} else {
		return a
	}
}

func MaxUint16(a uint16, b uint16) uint16 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinInt16(a int16, b int16) int16 {
	if a > b {
		return b
	} else {
		return a
	}
}

func MaxInt16(a int16, b int16) int16 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinUint32(a uint32, b uint32) uint32 {
	if a > b {
		return b
	} else {
		return a
	}
}

func MaxUint32(a uint32, b uint32) uint32 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinInt32(a int32, b int32) int32 {
	if a > b {
		return b
	} else {
		return a
	}
}

func MaxInt32(a int32, b int32) int32 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinUint64(a uint64, b uint64) uint64 {
	if a > b {
		return b
	} else {
		return a
	}
}

func MaxUint64(a uint64, b uint64) uint64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinInt64(a int64, b int64) int64 {
	if a > b {
		return b
	} else {
		return a
	}
}

func MaxInt64(a int64, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}
