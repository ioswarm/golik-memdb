package memdb

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/hashicorp/go-memdb"
	"github.com/ioswarm/golik"
)

func MemDB(system golik.Golik) (*MemDBService, error) {
	return NewMemDB("memdb", system)
}

func NewMemDB(name string, system golik.Golik) (*MemDBService, error) {
	mdb := &MemDBService{
		name: name,
		system: system,
	}

	hdl, err := system.ExecuteService(mdb)
	if err != nil {
		return nil, err
	}

	mdb.mutex.Lock()
	defer mdb.mutex.Unlock()
	mdb.handler = hdl

	return mdb, nil
}


type MemDBService struct {
	name     string
	system   golik.Golik
	handler  golik.CloveHandler

	mutex sync.Mutex
}

func (mdb *MemDBService) CreateServiceInstance(system golik.Golik) *golik.Clove {
	return &golik.Clove{
		Name: mdb.name,
		Behavior: func(ctx golik.CloveContext, msg golik.Message) {
			msg.Reply(golik.Done())
		},
	}
}

func (mdb *MemDBService) CreateConnectionPool(settings *golik.ConnectionPoolSettings) (golik.CloveRef, error) {
	itype := settings.Type
	if itype.Kind() != reflect.Struct {
		return nil, errors.New("Given type must be a struct")
	}
	if settings.Options == nil {
		settings.Options = make(map[string]interface{})
	}
	
	idx := settings.IndexField
	if idx == "" {
		if itype.NumField() == 0 {
			return nil, errors.New("Given type has no fields")
		}
		ftype := itype.Field(0)
		idx = golik.CamelCase(ftype.Name)
	}
	schema, err := CreateSingleDBSchema(itype, idx)
	if err != nil {
		return nil, err
	}
	database, err := memdb.NewMemDB(schema)
	if err != nil {
		return nil, err
	}
	if _, ok := settings.Options["memdb.database"]; !ok {
		settings.Options["memdb.database"] = database
	}
	if settings.CreateHandler == nil {
		table := firstTable(schema)
		if table == nil {
			return nil, errors.New("No table is defined")
		}
		index := firstUniquIndex(table)
		if index == nil {
			return nil, fmt.Errorf("No unique index defined for table %v", table.Name)
		}

		settings.CreateHandler = defaultHandlerCreation(database, table.Name, index.Name, settings.Behavior)
	}

	clove := golik.NewConnectionPool(settings)
	return mdb.handler.Execute(clove)
}