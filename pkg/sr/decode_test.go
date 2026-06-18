package sr

import (
	"encoding/binary"
	"testing"

	"google.golang.org/protobuf/proto"

	delightv1 "obs-svc/gen/go/delight/v1"
)

// frame mimics a producer (e.g. delightd) encoding a first-message type.
func frame(t *testing.T, schemaID int32, m proto.Message) []byte {
	t.Helper()
	payload, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := []byte{0x00}
	var id [4]byte
	binary.BigEndian.PutUint32(id[:], uint32(schemaID))
	out = append(out, id[:]...)
	out = append(out, 0x00) // message-index: first message in file
	return append(out, payload...)
}

func TestDecode_RoundTripsProducerFraming(t *testing.T) {
	ev := &delightv1.BackupEvent{ProjectName: "paling", Success: true, BytesAfter: 400}
	got, err := Decode(frame(t, 9, ev))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.SchemaID != 9 {
		t.Errorf("schema id = %d, want 9", got.SchemaID)
	}
	var back delightv1.BackupEvent
	if err := proto.Unmarshal(got.Payload, &back); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if back.GetProjectName() != "paling" || !back.GetSuccess() || back.GetBytesAfter() != 400 {
		t.Errorf("round-trip mismatch: %+v", &back)
	}
}

func TestDecode_Rejects(t *testing.T) {
	cases := map[string][]byte{
		"short":     {0x00, 0, 0},
		"bad magic": {0x01, 0, 0, 0, 1, 0},
	}
	for name, b := range cases {
		if _, err := Decode(b); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

// TestDecode_MultiMessageIndex exercises the non-optimized path: a message that
// is not first in its file is framed as [count][index...] rather than a single
// 0 byte.
func TestDecode_MultiMessageIndex(t *testing.T) {
	// magic + id=3 + index array [count=1, index=2] + payload 0xAA 0xBB
	b := []byte{0x00, 0, 0, 0, 3, 0x01, 0x02, 0xAA, 0xBB}
	got, err := Decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.SchemaID != 3 {
		t.Errorf("schema id = %d, want 3", got.SchemaID)
	}
	if len(got.Payload) != 2 || got.Payload[0] != 0xAA {
		t.Errorf("payload = %v, want [0xAA 0xBB]", got.Payload)
	}
}

// TestDecode_TruncatedIndex covers a frame that claims more index entries than
// it carries.
func TestDecode_TruncatedIndex(t *testing.T) {
	// count=5 but no index bytes follow
	if _, err := Decode([]byte{0x00, 0, 0, 0, 1, 0x05}); err == nil {
		t.Error("expected error on truncated message-index")
	}
}
