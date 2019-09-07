package persistence

import (
	"context"
	"errors"
	"reflect"
)

var (
	ErrNotFound = errors.New("persistence: data not found")
)

type Storer interface {
	Store(ctx context.Context, name string, data interface{}) error
}

type Updater interface {
	Update(context.Context, interface{}, string, interface{}) error
}

type Fetcher interface {
	Fetch(context.Context, interface{}, string, string, interface{}) error
}

type Persistence interface {
	Storer
	Fetcher
	Updater
}

type ProcessDataFunc func(p Persistence) error

func TxExecPersistence(p Persistence, processFunc ProcessDataFunc) error {

	pValue := reflect.ValueOf(p)
	beginVal := pValue.MethodByName("BeginTx")

	if !beginVal.IsValid() {
		return errors.New("invalid persistence type, cannot find method BeginTx")
	}
	beginResult := beginVal.Call([]reflect.Value{})
	if len(beginResult) == 1 && beginResult[0].Interface() != nil {
		theErr := beginResult[0].Interface().(error)
		return theErr
	}

	if err := processFunc(p); err != nil {
		rollbackVal := pValue.MethodByName("Rollback")
		if !rollbackVal.IsValid() {
			return errors.New("invalid persistence type, cannot find method Rollback")
		}
		rollbackResult := rollbackVal.Call([]reflect.Value{})
		if len(rollbackResult) == 1 && rollbackResult[0].Interface() != nil {
			theErr := rollbackResult[0].Interface().(error)
			return theErr
		}
		return err
	}

	commitVal := pValue.MethodByName("Commit")
	if !commitVal.IsValid() {
		return errors.New("invalid persistence type, cannot find method Commit")
	}
	commitResult := commitVal.Call([]reflect.Value{})
	if len(commitResult) == 1 && commitResult[0].Interface() != nil {
		theErr := commitResult[0].Interface().(error)
		return theErr
	}

	return nil
}
