package lsm

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestUnmarshalBloomRejectsInvalidMetadata(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "zero bit count",
			data: func() []byte {
				data := make([]byte, 16)
				binary.LittleEndian.PutUint64(data[0:8], 0)
				binary.LittleEndian.PutUint64(data[8:16], 1)
				return data
			}(),
		},
		{
			name: "bit count not byte aligned",
			data: func() []byte {
				data := make([]byte, 17)
				binary.LittleEndian.PutUint64(data[0:8], 9)
				binary.LittleEndian.PutUint64(data[8:16], 1)
				return data
			}(),
		},
		{
			name: "zero hash count",
			data: func() []byte {
				data := make([]byte, 24)
				binary.LittleEndian.PutUint64(data[0:8], 64)
				binary.LittleEndian.PutUint64(data[8:16], 0)
				return data
			}(),
		},
		{
			name: "bit array size mismatch",
			data: func() []byte {
				data := make([]byte, 23)
				binary.LittleEndian.PutUint64(data[0:8], 64)
				binary.LittleEndian.PutUint64(data[8:16], 1)
				return data
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := unmarshalBloom(tt.data)
			if !errors.Is(err, ErrCorruptionDetected) {
				t.Fatalf("expected ErrCorruptionDetected, got %v", err)
			}
		})
	}
}
