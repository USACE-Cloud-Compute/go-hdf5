package util

import (
	"encoding/binary"
	"math"
)

func F64fb(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

func F32fb(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	float := math.Float32frombits(bits)
	return float
}

func I32fb(bytes []byte) int32 {
	bits := binary.LittleEndian.Uint32(bytes)
	return int32(bits)
}

func I16fb(bytes []byte) int16 {
	bits := binary.LittleEndian.Uint16(bytes)
	return int16(bits)
}
