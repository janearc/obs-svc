// Package sr decodes the Confluent Schema Registry wire format. obs-svc-agg is
// the consumer counterpart to the producers' hand-rolled framing (e.g.
// delightd's): we do NOT get Confluent's deserializer here either, so the format
// is owned and documented in one place.
package sr

import (
	"encoding/binary"
	"fmt"
)

// Frame is a decoded Confluent payload: the registry schema id and the raw
// protobuf bytes that follow the framing.
type Frame struct {
	SchemaID int32
	Payload  []byte
}

// Decode strips the Confluent SR framing:
//
//	byte 0     : magic 0x00
//	bytes 1-4  : schema id, big-endian
//	bytes 5..  : message-index array -- a varint count followed by that many
//	             varint indexes. The single byte 0x00 is the Confluent
//	             optimization for the first message in a file ([0]); it decodes
//	             here as count=0, leaving the payload immediately after, which is
//	             the same byte boundary.
//	rest       : serialized protobuf payload
func Decode(b []byte) (Frame, error) {
	if len(b) < 6 {
		return Frame{}, fmt.Errorf("payload too short: %d bytes", len(b))
	}
	if b[0] != 0x00 {
		return Frame{}, fmt.Errorf("bad magic byte 0x%02x, want 0x00", b[0])
	}

	id := int32(binary.BigEndian.Uint32(b[1:5]))
	rest := b[5:]

	count, n := binary.Uvarint(rest)
	if n <= 0 {
		return Frame{}, fmt.Errorf("malformed message-index length")
	}
	rest = rest[n:]
	for i := uint64(0); i < count; i++ {
		_, m := binary.Uvarint(rest)
		if m <= 0 {
			return Frame{}, fmt.Errorf("malformed message-index entry %d", i)
		}
		rest = rest[m:]
	}

	return Frame{SchemaID: id, Payload: rest}, nil
}
