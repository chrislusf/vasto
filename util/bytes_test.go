package util

import (
	"testing"
)

func TestConversionUint64(t *testing.T) {

	x := uint64(12345)

	b := Uint64toBytes(x)

	if BytesToUint64(b) != x {
		t.Errorf("unexpected: %d, expecting %d", BytesToUint64(b), x)
	}

}

func TestConversionUint32(t *testing.T) {

	x := uint32(12345)

	b := Uint32toBytes(x)

	if BytesToUint32(b) != x {
		t.Errorf("unexpected: %d, expecting %d", BytesToUint32(b), x)
	}

}

func TestConversionUint16(t *testing.T) {

	x := uint16(12345)

	b := Uint16toBytes(x)

	if BytesToUint16(b) != x {
		t.Errorf("unexpected: %d, expecting %d", BytesToUint16(b), x)
	}

}

func TestConversionFloat64(t *testing.T) {

	x := float64(12345.678)

	b := Float64ToBytes(x)

	if BytesToFloat64(b) != x {
		t.Errorf("unexpected: %f, expecting %f", BytesToFloat64(b), x)
	}

}
