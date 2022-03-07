package net

import (
	"reflect"
)

type Encoder interface {
	Encode(e interface{}) error
	EncodeValue(value reflect.Value) error
}

type Decoder interface {
	Decode(e interface{}) error
}
