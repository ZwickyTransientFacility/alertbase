// Code generated by "stringer -type IndexDBType ."; DO NOT EDIT.

package benchutil

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[CandidateID-0]
	_ = x[SourceID-1]
	_ = x[Timestamp-2]
	_ = x[HEALPix-3]
}

const _IndexDBType_name = "CandidateIDSourceIDTimestampHEALPix"

var _IndexDBType_index = [...]uint8{0, 11, 19, 28, 35}

func (i IndexDBType) String() string {
	if i < 0 || i >= IndexDBType(len(_IndexDBType_index)-1) {
		return "IndexDBType(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _IndexDBType_name[_IndexDBType_index[i]:_IndexDBType_index[i+1]]
}
