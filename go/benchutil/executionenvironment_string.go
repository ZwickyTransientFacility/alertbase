// Code generated by "stringer -type ExecutionEnvironment ."; DO NOT EDIT.

package benchutil

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[Laptop-0]
}

const _ExecutionEnvironment_name = "Laptop"

var _ExecutionEnvironment_index = [...]uint8{0, 6}

func (i ExecutionEnvironment) String() string {
	if i < 0 || i >= ExecutionEnvironment(len(_ExecutionEnvironment_index)-1) {
		return "ExecutionEnvironment(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _ExecutionEnvironment_name[_ExecutionEnvironment_index[i]:_ExecutionEnvironment_index[i+1]]
}
