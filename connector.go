package connector

import (
	"context"
	"errors"
	"reflect"
)

type Connector interface {
	Connect(ctx context.Context) error
	Context() ConnectionContext
}

type ConnectionOption map[string]interface{}

func (o ConnectionOption) Add(key string, option interface{}) {
	o[key] = option
}

func (o ConnectionOption) Get(key string, output interface{}) error {
	ival, found := o[key]

	if !found {
		return errors.New("No value for key " + key)
	}

	oval := reflect.Indirect(reflect.ValueOf(output))
	if !oval.IsValid() {
		return errors.New("invalid output type")
	}

	if reflect.ValueOf(ival).Type().String() != oval.Type().String() {
		return errors.New("expected ;loutput has the same type with data option " + reflect.ValueOf(ival).Type().String())
	}

	oval.Set(reflect.ValueOf(ival))

	return nil
}

type ConnectionContext interface {
	Unwrap(actualContext interface{}) error
	Process(action func(input interface{}) error, input ...interface{}) error
}
