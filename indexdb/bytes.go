package indexdb

import (
	"encoding/binary"

	"github.com/ZwickyTransientFacility/alertbase/schema"
	"github.com/spenczar/healpix"
)

// packedUints64s provides a way to store a sequence of uint64s in a byte slice,
// the encoding used by LevelDB for values.
type packedUint64s []byte

func (a packedUint64s) Len() int {
	return len(a) / 8
}

func (a packedUint64s) Values() []uint64 {
	output := make([]uint64, a.Len())
	for i := 0; i < a.Len(); i++ {
		output[i] = binary.BigEndian.Uint64(a[i*8 : (i+1)*8])
	}
	return output
}

func (a packedUint64s) Append(v uint64) packedUint64s {
	return packedUint64s(append(a, uint64ToBytes(v)...))
}

func uint64ToBytes(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

func byteID(a *schema.Alert) []byte {
	return uint64ToBytes(uint64(a.Candid))
}

func byteObjectID(a *schema.Alert) []byte {
	return []byte(a.ObjectId)
}

func byteTimestamp(a *schema.Alert) []byte {
	return uint64ToBytes(jd2unix(a.Candidate.Jd))
}

func byteHEALPixel(a *schema.Alert, m *healpix.HEALPixMapper) []byte {
	pointing := healpix.RADec(a.Candidate.Ra, a.Candidate.Dec)
	pixel := m.PixelAt(pointing)
	return uint64ToBytes(uint64(pixel))
}

// jd2unix converts a Julian Date to a Unix Nanosecond Timestamp (doesn't
// attempt to handle leap seconds).
func jd2unix(jd float64) uint64 {
	return uint64((jd - 2440587.5) * 86400000000000)
}
