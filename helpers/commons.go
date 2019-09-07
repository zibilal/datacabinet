package helpers

import (
	"reflect"
)

func ValidateType(val1, val2 reflect.Value) bool {
	return (val1.IsValid() && val2.IsValid()) && (val1.Type().String() == val2.Type().String())
}
