package mongoconnector

import (
	"context"
	"errors"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
	"github.com/mongodb/mongo-go-driver/mongo/readpref"
	"github.com/zibilal/datacabinet"
	"github.com/zibilal/datacabinet/helpers"
	"reflect"
)

const (
	ReadPrefPrimary   = "primary"
	ReadPrefSecondary = "secondary"
)

type MongodbConnector struct {
	connectionContext connector.ConnectionContext
	readPref          string
}

func NewMongodbConnector(ctx context.Context, uri string) (*MongodbConnector, error) {
	c := new(MongodbConnector)

	client, err := mongo.Connect(ctx, uri)
	if err != nil {
		return nil, err
	}
	c.connectionContext = NewMongodbConnectorContext(client)
	return c, nil
}

func NewMongodbConnectorWithReadMode(ctx context.Context, uri string, readPref string) (*MongodbConnector, error) {
	c := new(MongodbConnector)

	co := options.Client()

	if readPref == ReadPrefSecondary {
		co.SetReadPreference(readpref.Secondary())
	} else {
		co.SetReadPreference(readpref.Primary())
	}
	client, err := mongo.Connect(ctx, uri, co)
	if err != nil {
		return nil, err
	}

	c.connectionContext = NewMongodbConnectorContext(client)
	return c, nil
}

func (c *MongodbConnector) Connect(ctx context.Context) error {

	var client mongo.Client
	err := c.connectionContext.Unwrap(&client)
	if err != nil {
		return err
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *MongodbConnector) Context() connector.ConnectionContext {
	return c.connectionContext
}

type MongodbConnectorContext struct {
	mongoClient *mongo.Client
}

func NewMongodbConnectorContext(mongoClient *mongo.Client) *MongodbConnectorContext {
	connCtx := new(MongodbConnectorContext)
	connCtx.mongoClient = mongoClient
	return connCtx
}

func (c *MongodbConnectorContext) Unwrap(actualContext interface{}) error {
	oval := reflect.Indirect(reflect.ValueOf(actualContext))
	ival := reflect.Indirect(reflect.ValueOf(c.mongoClient))

	if !helpers.ValidateType(ival, oval) {
		return errors.New("unknown context type " + oval.Type().String())
	}

	oval.Set(ival)

	return nil
}

func (c *MongodbConnectorContext) Process(action func(input interface{}) error, input ...interface{}) error {
	var err error

	if len(input) > 0 {
		for _, i := range input {
			err = action(i)
			if err != nil {
				return err
			}
		}
	} else {
		err = action(nil)
		if err != nil {
			return err
		}
	}

	return nil
}
