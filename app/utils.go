package main

import (
	"cmp"
	"log"
	"slices"
)

func readBigEndianUint16(b []byte) uint16 {
	return uint16(b[0])<<8 | uint16(b[1])
}

func readBigEndianUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func readBigEndianVarint(data []byte) (value int64, size int) {
	for size < 9 {
		size++
		if size == 9 {
			value = (value << 8) | int64(data[size-1])
			break
		} else {
			value = (value << 7) | (int64(data[size-1]) & 0b01111111)
		}
		if (data[size-1]>>7)&1 == 0 {
			break
		}
	}
	return
}

func readBigEndianInt(data []byte) (value int64) {
	for _, b := range data {
		value = (value << 8) | int64(b)
	}
	return value
}

func compareAny(a any, b any) int {
	if a == nil && b == nil {
		return 0
	} else if a == nil || b == nil {
		return -1
	}
	switch a.(type) {
	case string:
		switch b.(type) {
		case string:
			return cmp.Compare(a.(string), b.(string))
		case []byte:
			return cmp.Compare(a.(string), string(b.([]byte)))
		}
	case int64:
		switch b.(type) {
		case int64:
			return cmp.Compare(a.(int64), b.(int64))
		case float64:
			return cmp.Compare(float64(a.(int64)), b.(float64))
		}
	case float64:
		switch b.(type) {
		case float64:
			return cmp.Compare(a.(float64), b.(float64))
		case int64:
			return cmp.Compare(a.(float64), float64(b.(int64)))
		}
	case []byte:
		switch b.(type) {
		case string:
			return cmp.Compare(string(a.([]byte)), b.(string))
		case []byte:
			return slices.Compare(a.([]byte), b.([]byte))
		}
	}
	log.Fatalf("no comparison for types: %T and %T", a, b)
	return -1
}
