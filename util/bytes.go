package util

import (
	"encoding/binary"
	"math"
)

// big endian

// BytesToUint64 converts big endian 8 bytes into uint64
func BytesToUint64(b []byte) (v uint64) {
	length := uint(len(b))
	for i := uint(0); i < length-1; i++ {
		v += uint64(b[i])
		v <<= 8
	}
	v += uint64(b[length-1])
	return
}

// BytesToUint32 converts big endian 4 bytes into uint32
func BytesToUint32(b []byte) (v uint32) {
	length := uint(len(b))
	for i := uint(0); i < length-1; i++ {
		v += uint32(b[i])
		v <<= 8
	}
	v += uint32(b[length-1])
	return
}

// BytesToUint16 converts big endian 2 bytes into uint16
func BytesToUint16(b []byte) (v uint16) {
	v += uint16(b[0])
	v <<= 8
	v += uint16(b[1])
	return
}

// Uint64toBytes converts uint64 into big endian 8 bytes
func Uint64toBytes(v uint64) (b []byte) {
	b = make([]byte, 8)
	for i := uint(0); i < 8; i++ {
		b[7-i] = byte(v >> (i * 8))
	}
	return b
}

// Uint32toBytes converts uint32 into big endian 4 bytes
func Uint32toBytes(v uint32) (b []byte) {
	b = make([]byte, 4)
	for i := uint(0); i < 4; i++ {
		b[3-i] = byte(v >> (i * 8))
	}
	return b
}

// Uint16toBytes converts uint16 into big endian 2 bytes
func Uint16toBytes(v uint16) (b []byte) {
	b = make([]byte, 2)
	b[0] = byte(v >> 8)
	b[1] = byte(v)
	return b
}

// float64 converts from little endian 8 bytes
func BytesToFloat64(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

// float64 converts to little endian 8 bytes
func Float64ToBytes(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}
