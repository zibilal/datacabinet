package mongoconnector

import (
	"github.com/mongodb/mongo-go-driver/mongo"
	"testing"
)

const (
	success = "\u2713"
	failed  = "\u2717"
)

func TestNewMongodbConnectorContext(t *testing.T) {
	t.Log("Test MongodbConnectorContext")
	{

		connectionString := "mongodb://data.example.com"
		mongoClient, _ := mongo.NewClient(connectionString)
		connectorContext := NewMongodbConnectorContext(mongoClient)

		var mongoContext mongo.Client

		err := connectorContext.Unwrap(&mongoContext)
		if err != nil {
			t.Fatalf("%s expected error nil, got %s", failed, err.Error())
		}

		t.Logf("%s expected error nil", success)

		if mongoContext.ConnectionString() != connectionString {
			t.Fatalf("%s expected connection string == %s, got %s", failed, connectionString, mongoContext.ConnectionString())
		}

		t.Logf("%s expected connection string == %s", success, connectionString)
	}
}
