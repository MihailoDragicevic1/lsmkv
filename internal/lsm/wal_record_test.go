package lsm

import (
	"testing"
)

func TestWALRecordEncodeDecodePut(t *testing.T) {
	record := WALRecord{
		Op:    WALOpPut,
		SeqNo: 42,
		Key:   []byte("user:123"),
		Value: []byte("hello"),
	}

	encoded, err := record.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := DecodeWALRecord(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Op != record.Op {
		t.Fatalf("unexpected op: got %d, want %d", decoded.Op, record.Op)
	}

	if decoded.SeqNo != record.SeqNo {
		t.Fatalf("unexpected seqno: got %d, want %d", decoded.SeqNo, record.SeqNo)
	}

	if string(decoded.Key) != string(record.Key) {
		t.Fatalf("unexpected key: got %q, want %q", decoded.Key, record.Key)
	}

	if string(decoded.Value) != string(record.Value) {
		t.Fatalf("unexpected value: got %q, want %q", decoded.Value, record.Value)
	}
}

func TestWALRecordEncodeDecodeDelete(t *testing.T) {
	record := WALRecord{
		Op:    WALOpDel,
		SeqNo: 99,
		Key:   []byte("user:456"),
	}

	encoded, err := record.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := DecodeWALRecord(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Op != record.Op {
		t.Fatalf("unexpected op: got %d, want %d", decoded.Op, record.Op)
	}

	if decoded.SeqNo != record.SeqNo {
		t.Fatalf("unexpected seqno: got %d, want %d", decoded.SeqNo, record.SeqNo)
	}

	if string(decoded.Key) != string(record.Key) {
		t.Fatalf("unexpected key: got %q, want %q", decoded.Key, record.Key)
	}

	if len(decoded.Value) != 0 {
		t.Fatalf("delete record should have empty value, got len=%d", len(decoded.Value))
	}
}

func TestWALRecordInvalidOp(t *testing.T) {
	record := WALRecord{
		Op:    0,
		SeqNo: 1,
		Key:   []byte("k"),
	}

	if _, err := record.Encode(); err == nil {
		t.Fatalf("expected error for invalid op, got nil")
	}
}

func TestDecodeWALRecordTruncated(t *testing.T) {
	record := WALRecord{
		Op:    WALOpPut,
		SeqNo: 1,
		Key:   []byte("k"),
		Value: []byte("v"),
	}

	encoded, err := record.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	truncated := encoded[:len(encoded)-1]

	if _, err := DecodeWALRecord(truncated); err == nil {
		t.Fatalf("expected error for truncated record, got nil")
	}
}
