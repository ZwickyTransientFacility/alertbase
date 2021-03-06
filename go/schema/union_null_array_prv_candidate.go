// Code generated by github.com/actgardner/gogen-avro/v7. DO NOT EDIT.
/*
 * SOURCES:
 *     alert.avsc
 *     candidate.avsc
 *     cutout.avsc
 *     prv_candidate.avsc
 */
package schema

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/actgardner/gogen-avro/v7/vm"
	"github.com/actgardner/gogen-avro/v7/vm/types"
)

type UnionNullArrayPrv_candidateTypeEnum int

const (
	UnionNullArrayPrv_candidateTypeEnumArrayPrv_candidate UnionNullArrayPrv_candidateTypeEnum = 1
)

type UnionNullArrayPrv_candidate struct {
	Null               *types.NullVal
	ArrayPrv_candidate []*Prv_candidate
	UnionType          UnionNullArrayPrv_candidateTypeEnum
}

func writeUnionNullArrayPrv_candidate(r *UnionNullArrayPrv_candidate, w io.Writer) error {

	if r == nil {
		err := vm.WriteLong(0, w)
		return err
	}

	err := vm.WriteLong(int64(r.UnionType), w)
	if err != nil {
		return err
	}
	switch r.UnionType {
	case UnionNullArrayPrv_candidateTypeEnumArrayPrv_candidate:
		return writeArrayPrv_candidate(r.ArrayPrv_candidate, w)
	}
	return fmt.Errorf("invalid value for *UnionNullArrayPrv_candidate")
}

func NewUnionNullArrayPrv_candidate() *UnionNullArrayPrv_candidate {
	return &UnionNullArrayPrv_candidate{}
}

func (_ *UnionNullArrayPrv_candidate) SetBoolean(v bool)   { panic("Unsupported operation") }
func (_ *UnionNullArrayPrv_candidate) SetInt(v int32)      { panic("Unsupported operation") }
func (_ *UnionNullArrayPrv_candidate) SetFloat(v float32)  { panic("Unsupported operation") }
func (_ *UnionNullArrayPrv_candidate) SetDouble(v float64) { panic("Unsupported operation") }
func (_ *UnionNullArrayPrv_candidate) SetBytes(v []byte)   { panic("Unsupported operation") }
func (_ *UnionNullArrayPrv_candidate) SetString(v string)  { panic("Unsupported operation") }
func (r *UnionNullArrayPrv_candidate) SetLong(v int64) {
	r.UnionType = (UnionNullArrayPrv_candidateTypeEnum)(v)
}
func (r *UnionNullArrayPrv_candidate) Get(i int) types.Field {
	switch i {
	case 0:
		return r.Null
	case 1:
		r.ArrayPrv_candidate = make([]*Prv_candidate, 0)
		return &ArrayPrv_candidateWrapper{Target: (&r.ArrayPrv_candidate)}
	}
	panic("Unknown field index")
}
func (_ *UnionNullArrayPrv_candidate) NullField(i int)  { panic("Unsupported operation") }
func (_ *UnionNullArrayPrv_candidate) SetDefault(i int) { panic("Unsupported operation") }
func (_ *UnionNullArrayPrv_candidate) AppendMap(key string) types.Field {
	panic("Unsupported operation")
}
func (_ *UnionNullArrayPrv_candidate) AppendArray() types.Field { panic("Unsupported operation") }
func (_ *UnionNullArrayPrv_candidate) Finalize()                {}

func (r *UnionNullArrayPrv_candidate) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	switch r.UnionType {
	case UnionNullArrayPrv_candidateTypeEnumArrayPrv_candidate:
		return json.Marshal(map[string]interface{}{"array": r.ArrayPrv_candidate})
	}
	return nil, fmt.Errorf("invalid value for *UnionNullArrayPrv_candidate")
}

func (r *UnionNullArrayPrv_candidate) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if value, ok := fields["array"]; ok {
		r.UnionType = 1
		return json.Unmarshal([]byte(value), &r.ArrayPrv_candidate)
	}
	return fmt.Errorf("invalid value for *UnionNullArrayPrv_candidate")
}
