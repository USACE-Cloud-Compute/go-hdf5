package util

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	hdf5 "github.com/usace-cloud-compute/go-hdf5"
)

const stringLengthTag = "strlen"

type UnpackFunction func(b []byte) reflect.Value

func ReadCompoundAttributes(f *hdf5.File, attrpath string, data interface{}, uf UnpackFunction) error {
	dset, err := f.OpenDataset(attrpath)
	if err != nil {
		return err
	}
	defer dset.Close()

	err = unmarshalCompoundType(dset, data, uf)
	return err
}

func unmarshalCompoundType(dset *hdf5.Dataset, out interface{}, uf UnpackFunction) error {
	v := reflect.ValueOf(out).Elem()
	dsetDim, err := getLength(dset)
	if err != nil {
		return err
	}

	typ := reflect.TypeOf(out).Elem().Elem()
	metadata, err := compoundAttrMetadata(dset, typ)
	if err != nil {
		return err
	}

	packedSize := metadata.PackedSize()
	hdf5Raw := make([]byte, packedSize*int(dsetDim))
	err = dset.Read(&hdf5Raw)
	if err != nil {
		return err
	}

	v.Set(reflect.MakeSlice(reflect.SliceOf(typ), int(dsetDim), int(dsetDim)))
	var count int = 0
	for i := 0; i < len(hdf5Raw); i += packedSize {
		var val reflect.Value
		if uf == nil {
			val, err = unpack(typ, hdf5Raw[i:packedSize+i], metadata)
		} else {
			val = uf(hdf5Raw[i : packedSize+i])
		}
		if err != nil {
			return err
		}
		//
		//unpack other dsets here....
		//
		s := v.Index(count)
		s.Set(val)
		count++
	}
	return nil
}

func unpack(t reflect.Type, b []byte, metadata CompoundAttributeMetadata) (reflect.Value, error) {
	newval := reflect.New(t).Elem()
	bytecnt := 0
	for _, v := range metadata.UT {
		field := newval.Field(v.TargetIndex)
		switch field.Type().Kind() {
		case reflect.Uint8:
			field.SetUint(uint64(b[bytecnt : bytecnt+1][0]))
			bytecnt += 1
		case reflect.Int32:
			field.SetInt(int64(I32fb(b[bytecnt : bytecnt+4])))
			bytecnt += 4
		case reflect.Float32:
			field.SetFloat(float64(F32fb(b[bytecnt : bytecnt+4])))
			bytecnt += 4
		case reflect.Float64:
			field.SetFloat(float64(F64fb(b[bytecnt : bytecnt+8])))
			bytecnt += 4
		case reflect.String:
			stl := v.Len
			strval := string(bytes.Trim(b[bytecnt:bytecnt+stl], "\x00")) //slice and strip out all null character values
			newval.Field(v.TargetIndex).SetString(strval)
			bytecnt += stl
		}
	}
	return newval, nil
}

var typesize map[reflect.Kind]int = map[reflect.Kind]int{
	reflect.Uint8:   1,
	reflect.Int32:   4,
	reflect.Float32: 4,
	reflect.Float64: 8,
}

type FieldMetadata struct {
	FieldName  string
	FieldIndex int
	FieldType  reflect.Kind
	StringSize int
	HdfName    string
}

type unpackTable struct {
	ValueIndex  int
	TargetIndex int
	Len         int
}

type CompoundAttributeMetadata struct {
	NumFields  int
	FieldNames []string
	Dest       []FieldMetadata
	UT         []unpackTable
}

func compoundAttrMetadata(dset *hdf5.Dataset, dest reflect.Type) (CompoundAttributeMetadata, error) {
	typ, err := dset.Datatype()
	if err != nil {
		return CompoundAttributeMetadata{}, err
	}
	ctype := hdf5.CompoundType{*typ}
	nm := ctype.NMembers()
	names := make([]string, nm)
	for i := 0; i < nm; i++ {
		names[i] = strings.TrimSpace(ctype.MemberName(i))
	}

	numDestfields := dest.NumField()
	fieldMetadata := make([]FieldMetadata, numDestfields)
	for j := 0; j < numDestfields; j++ {
		f := dest.Field(j)

		fm := FieldMetadata{
			FieldName:  f.Name,
			FieldIndex: j,
			FieldType:  f.Type.Kind(),
		}
		if hdf, ok := f.Tag.Lookup("hdf"); ok {
			fm.HdfName = hdf
		}
		if strlen, ok := f.Tag.Lookup("strlen"); ok {
			strsize, err := strconv.Atoi(strlen)
			if err != nil {
				return CompoundAttributeMetadata{}, errors.New("Invalid string length")
			}
			fm.StringSize = strsize
		}
		fieldMetadata[j] = fm
	}

	cam := CompoundAttributeMetadata{
		NumFields:  nm,
		FieldNames: names,
		Dest:       fieldMetadata,
	}
	err = cam.BuildUnpackTable()
	return cam, err
}

func (cam *CompoundAttributeMetadata) destType(hdfName string) (FieldMetadata, error) {
	for _, v := range cam.Dest {
		if v.HdfName == hdfName {
			return v, nil
		}
	}
	return FieldMetadata{}, errors.New(fmt.Sprintf("Invalid field in the hdf dataset: %s", hdfName))
}

func (cam *CompoundAttributeMetadata) BuildUnpackTable() error {
	ut := []unpackTable{}
	for i, fn := range cam.FieldNames {
		fm, err := cam.destType(fn)
		if err != nil {
			return err
		}
		ut = append(ut, unpackTable{i, fm.FieldIndex, typeSize(fm)})
	}
	cam.UT = ut
	return nil
}

func (cam *CompoundAttributeMetadata) PackedSize() int {
	var size int
	for _, v := range cam.UT {
		size += v.Len
	}
	return size
}

func typeSize(fm FieldMetadata) int {
	if fm.FieldType == reflect.String {
		return fm.StringSize
	} else {
		return typesize[fm.FieldType]
	}
}

func getLength(dset *hdf5.Dataset) (uint, error) {
	space := dset.Space()
	dims, _, err := space.SimpleExtentDims()
	if err != nil {
		return 0, err
	}
	return dims[0], nil
}

func sum(array []int) int {
	result := 0
	for _, v := range array {
		result += v
	}
	return result
}
