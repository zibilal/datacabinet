package mongodbpersistence

import (
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"testing"
)

const (
	success = "\u2713"
	failed  = "\u2717"
)

func TestQueryMap_SetObjectId(t *testing.T) {
	t.Log("QueryMap SetObjectId")
	{
		str := "5c9b503a0459a8137a8a409f"
		queryMap := QueryMap{}
		err := queryMap.SetObjectId(str)

		if err != nil {
			t.Fatalf("%s expected error == nil, got %s", failed, err.Error())
		}

		istr, found := queryMap["_id"].(primitive.ObjectID)

		if !found {
			t.Fatalf("%s unexpected type", failed)
		}

		if str == istr.Hex() {
			t.Logf("%s expected _id == %s", success, str)
		} else {
			t.Errorf("%s expected _id == %s, got %s", failed, str, istr)
		}
	}
}

func TestQueryMap_FromStruct(t *testing.T) {
	t.Log("QueryMap FromStruct _id")
	{
		dt := struct {
			Id   string  `query:"_id"`
			Name string  `query:"full_name"`
			Rate float64 `query:"rate"`
		}{
			Id: "5c9b503a0459a8137a8a409f",
		}

		queryMap := QueryMap{}
		err := queryMap.FromStruct(dt)
		if err != nil {
			t.Fatalf("%s expected error not nil, got %s", failed, err.Error())
		}

		dtId, found := queryMap["_id"].(primitive.ObjectID)
		if !found {
			t.Fatalf("%s expected _id is found", failed)
		}

		if dtId.Hex() == dt.Id {
			t.Logf("%s expected Id == %s", success, dt.Id)
		} else {
			t.Fatalf("%s expected Id == %s", failed, dt.Id)
		}

		_, found = queryMap["full_name"]

		if !found {
			t.Logf("%s expected full_name is not found", success)
		} else {
			t.Fatalf("%s expected full_name is not found", failed)
		}
	}

	t.Log("QueryMap FromStruct Full Name and Rate")
	{
		dt := struct {
			Id   string  `query:"_id"`
			Name string  `query:"full_name"`
			Rate float64 `query:"rate"`
		}{
			Name: "test name",
			Rate: 0.255,
		}

		queryMap := QueryMap{}
		err := queryMap.FromStruct(dt)
		if err != nil {
			t.Fatalf("%s expected error nil, got %s", failed, err.Error())
		}

		_, found := queryMap["full_name"]
		if found {
			t.Logf("%s expected full_name is found", success)
		} else {
			t.Errorf("%s expected full_name is found", failed)
		}

		_, found = queryMap["rate"]
		if found {
			t.Logf("%s expected rate is found", success)
		} else {
			t.Errorf("%s expected rate is found", failed)
		}

		_, found = queryMap["_id"]
		if !found {
			t.Logf("%s expected rate is not found", success)
		} else {
			t.Errorf("%s expected rate is not found", failed)
		}
	}
}
