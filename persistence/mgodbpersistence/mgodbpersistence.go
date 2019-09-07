package mgodbpersistence

import (
	"context"
	"errors"
	"github.com/zibilal/datacabinet"

	"gopkg.in/mgo.v2"
)

type MgoPersistence struct {
	connectionContext connector.ConnectionContext
	dbName            string
}

func NewMgoPersistence(connectionContext connector.ConnectionContext, dbName string) *MgoPersistence {
	p := new(MgoPersistence)
	p.dbName = dbName
	p.connectionContext = connectionContext
	return p
}

func (m *MgoPersistence) Store(ctx context.Context, name string, data interface{}) error {
	err := m.connectionContext.Process(func(input interface{}) error {
		var mongoSession mgo.Session
		err := m.connectionContext.Unwrap(&mongoSession)
		if err != nil {
			return err
		}

		sCopy := mongoSession.Copy()
		defer sCopy.Close()

		return sCopy.DB(m.dbName).C(name).Insert(data)
	})

	if err != nil {
		return err
	}

	return nil
}

func (m *MgoPersistence) Update(ctx context.Context, query interface{}, name string, data interface{}) error {
	err := m.connectionContext.Process(func(input interface{}) error {
		var mongoSession mgo.Session
		err := m.connectionContext.Unwrap(&mongoSession)
		if err != nil {
			return err
		}

		sCopy := mongoSession.Copy()
		defer sCopy.Close()

		return sCopy.DB(m.dbName).C(name).Update(query, data)
	})

	if err != nil {
		return err
	}

	return nil
}

func (m *MgoPersistence) Fetch(ctx context.Context, query interface{}, fetchType, name string, output interface{}) error {
	err := m.connectionContext.Process(func(input interface{}) error {
		var mongoSession mgo.Session
		err := m.connectionContext.Unwrap(&mongoSession)
		if err != nil {
			return err
		}

		sCopy := mongoSession.Copy()
		defer sCopy.Close()

		queryMap, ok := query.(map[string]interface{})
		if !ok {
			return errors.New("[MgoPersistence.Fetch]unexpected type")
		}

		if fetchType == AggregateFetch {
			return sCopy.DB(m.dbName).C(name).Find(queryMap).All(output)
		} else {
			return sCopy.DB(m.dbName).C(name).Pipe(queryMap).All(output)
		}
	})

	if err != nil {
		return err
	}

	return nil
}

const (
	AggregateFetch = "aggregate"
)
