package net

import (
	"birdkv/test-tool/labgob"
	"io"
	"reflect"
)

type GobEncoder struct {
	*labgob.LabEncoder
}

func NewGobEncoder(w io.Writer) *GobEncoder {
	return &GobEncoder{
		labgob.NewEncoder(w),
	}
}

func (g *GobEncoder) Encode(e interface{}) error {
	return g.LabEncoder.Encode(e)
}

func (g *GobEncoder) EncodeValue(value reflect.Value) error {
	return g.LabEncoder.EncodeValue(value)
}

type GobDecoder struct {
	*labgob.LabDecoder
}

func NewGobDecoder(r io.Reader) *GobDecoder {
	return &GobDecoder{
		labgob.NewDecoder(r),
	}
}

func (g *GobDecoder) Decode(e interface{}) error {
	return g.LabDecoder.Decode(e)
}
