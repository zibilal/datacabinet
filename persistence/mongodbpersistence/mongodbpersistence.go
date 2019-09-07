package mongodbpersistence

import (
	"context"
	"errors"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
	"github.com/zibilal/datacabinet"
	"reflect"
	"strings"
)

type MongoPersistence struct {
	connectionContext connector.ConnectionContext
	dbName            string
}

func NewMongoPersistence(connectionContext connector.ConnectionContext, dbName string) *MongoPersistence {
	p := new(MongoPersistence)
	p.dbName = dbName
	p.connectionContext = connectionContext
	return p
}

func (m *MongoPersistence) Store(ctx context.Context, name string, data interface{}) error {
	err := m.connectionContext.Process(func(input interface{}) error {
		var mongoSession mongo.Client
		err := m.connectionContext.Unwrap(&mongoSession)
		if err != nil {
			return err
		}

		_, err = mongoSession.Database(m.dbName).Collection(name).InsertOne(ctx, data)
		if err != nil {
			return err
		}

		return nil
	}, data)

	if err != nil {
		return err
	}

	return nil
}

func (m *MongoPersistence) Update(ctx context.Context, query interface{}, name string, data interface{}) error {
	err := m.connectionContext.Process(func(input interface{}) error {
		var mongoSession mongo.Client
		err := m.connectionContext.Unwrap(&mongoSession)
		if err != nil {
			return err
		}

		dataM, err := fromStructToCommandDocument("$set", data)
		if err != nil {
			return err
		}

		_, err = mongoSession.Database(m.dbName).Collection(name).UpdateOne(ctx, query, dataM)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (m *MongoPersistence) Fetch(ctx context.Context, query interface{}, fetchType, name string, output interface{}) error {

	queryMap, ok := query.(map[string]interface{})
	if !ok {
		return errors.New("query should of type map[string]interface{}")
	}

	err := m.connectionContext.Process(func(input interface{}) error {
		var mongoSession mongo.Client
		err := m.connectionContext.Unwrap(&mongoSession)
		if err != nil {
			return err
		}

		otyp := reflect.TypeOf(output)

		// check the output type
		// 1. does the type is pointer for slice
		// 2. does the type is pointer for a struct
		// 3. else return an error

		var elemType reflect.Type
		var elemOutput reflect.Type
		if otyp.Kind() == reflect.Ptr {
			if otyp.Elem().Kind() != reflect.Slice && otyp.Elem().Kind() != reflect.Struct {
				return errors.New("expected output as type of pointer of slice or a struct")
			} else if otyp.Elem().Kind() == reflect.Slice {
				elemType = otyp.Elem().Elem()
				elemOutput = otyp.Elem()
			} else if otyp.Elem().Kind() == reflect.Struct {
				elemType = otyp.Elem()
				elemOutput = otyp.Elem()
			}
		} else {
			return errors.New("expected output as type of pointer")
		}
		//////
		var (
			cursor *mongo.Cursor
		)

		page, _ := queryMap["page"].(int)
		limit, _ := queryMap["limit"].(int)
		sorting, _ := queryMap["sorting"].(string)

		delete(queryMap, "page")
		delete(queryMap, "limit")
		delete(queryMap, "sorting")

		if fetchType == "aggregate" {
			cursor, err = mongoSession.Database(m.dbName).Collection(name).Aggregate(ctx, query)
		} else if fetchType == "find" {
			findOptions := options.Find()
			if limit > 0 {
				findOptions.SetLimit(int64(limit))
			}
			if page > 0 {
				skip := limit * (page - 1)
				findOptions.SetSkip(int64(skip))
			}

			ortSplit := strings.Split(sorting, ",")
			if len(ortSplit) == 2 && ortSplit[1] == "desc" {
				sortMap := make(map[string]interface{})
				sortMap[ortSplit[0]] = -1
				findOptions.SetSort(sortMap)
			} else if sorting == "asc" {
				sortMap := make(map[string]interface{})
				sortMap[ortSplit[0]] = 1
				findOptions.SetSort(sortMap)
			}

			cursor, err = mongoSession.Database(m.dbName).Collection(name).Find(ctx, query, findOptions)
			if err != nil {
				return err
			}
		} else {
			return errors.New("only accept fetch type aggregate or find")
		}

		if err != nil {
			return err
		}

		isEmpty := true
		for cursor.Next(ctx) {
			isEmpty = false
			aval := reflect.New(elemType)
			err = cursor.Decode(aval.Interface())

			if err != nil {
				return err
			}

			if elemOutput.Kind() == reflect.Slice {
				oval := reflect.ValueOf(output).Elem()
				oval = reflect.Append(oval, reflect.Indirect(aval))
				reflect.ValueOf(output).Elem().Set(oval)
			} else if elemOutput.Kind() == reflect.Struct {
				reflect.ValueOf(output).Elem().Set(reflect.Indirect(aval))
				break
			}
		}

		if isEmpty {
			return errors.New("data not found")
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

type QueryMap map[string]interface{}

func (m QueryMap) SetObjectId(id string) error {
	var err error
	m["_id"], err = primitive.ObjectIDFromHex(id)

	return err
}

func (m QueryMap) KeyValue(key string, val interface{}) {
	m[key] = val
}

func (m QueryMap) FromStruct(input interface{}) error {
	ityp := reflect.TypeOf(input)
	if ityp.Kind() == reflect.Struct {
		ival := reflect.ValueOf(input)
		for i := 0; i < ityp.NumField(); i++ {
			ftyp := ityp.Field(i)
			itag := ftyp.Tag.Get("query")

			if itag == "_id" {
				if ftyp.Type.Kind() != reflect.String {
					return errors.New("valid _id only of value string")
				}

				fval := ival.Field(i)
				if fval.IsValid() {
					sval, found := fval.Interface().(string)
					if found && sval != "" {
						return m.SetObjectId(fval.Interface().(string))
					}
				}
			} else if itag != "" {
				if ival.Field(i).IsValid() {
					m[itag] = ival.Field(i).Interface()
				}
			}
		}

		return nil
	}

	return errors.New("only accept input of type simple struct")
}

func fromStructToCommandDocument(command string, input interface{}) (bson.M, error) {
	ityp := reflect.TypeOf(input)
	if ityp.Kind() == reflect.Struct {
		inside := bson.M{}
		ival := reflect.ValueOf(input)
		for i := 0; i < ityp.NumField(); i++ {
			ftyp := ityp.Field(i)
			itag := ftyp.Tag.Get("bson")
			isplit := strings.Split(itag, ",")

			if len(isplit) > 0 {
				if isplit[0] == "_id" {
					continue
				} else if isplit[0] != "" {
					if ival.Field(i).IsValid() {
						inside[itag] = ival.Field(i).Interface()
					}
				}
			}

		}

		result := bson.M{}
		result[command] = inside

		return result, nil
	}

	return bson.M{}, errors.New("only accept input of type simple struct")
}
