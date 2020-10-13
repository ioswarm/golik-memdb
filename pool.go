package memdb

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/hashicorp/go-memdb"
	"github.com/ioswarm/golik"
	"github.com/ioswarm/golik/persistance"
)

func NewMemDBPoolSettingsOf(ttype reflect.Type, index string) (*MemDBPoolSettings, error) {
	schema, err := CreateSingleDBSchema(ttype, index)
	if err != nil {
		return nil, err
	}
	return NewMemDBPoolSettings(schema)
}

func NewMemDBPoolSettings(schema *memdb.DBSchema) (*MemDBPoolSettings, error) {
	for tkey := range schema.Tables {
		tbl := schema.Tables[tkey]
		for ikey := range tbl.Indexes {
			idx := tbl.Indexes[ikey]
			if idx.Unique {
				return &MemDBPoolSettings{
					Table: tbl.Name,
					Index: idx.Name,
					Schema: schema,
				}, nil
			}
		}
		return nil, fmt.Errorf("No unique index defined in table-schema %v", tkey)
	}
	return nil, errors.New("No table defined in schema")
}

type MemDBPoolSettings struct {
	Table string
	Index string
	Schema *memdb.DBSchema
	Size int
	Behavior func(*memdb.MemDB) struct{}

	db *memdb.MemDB
	mutex sync.Mutex
}

func (db *MemDBPoolSettings) Name() string {
	return db.Table
}

func (db *MemDBPoolSettings) PoolSize() int {
	if db.Size == 0 {
		return 10
	}
	return db.Size
}

func (db *MemDBPoolSettings) Connect(ctx golik.CloveContext) error {
	mdb, err := memdb.NewMemDB(db.Schema)
	if err != nil {
		ctx.Error("Error while create memDB: %v", err)
		return err
	}

	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.db = mdb
	
	ctx.Info("MemDB %v created", db.Name())

	return nil
}

func (db *MemDBPoolSettings) Close(golik.CloveContext) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.db = nil

	return nil
}

func (db *MemDBPoolSettings) createBehavior() interface{} {
	if db.Behavior == nil {
		return nil
	}
	return db.Behavior(db.db)
}

func (db *MemDBPoolSettings) CreateHandler(ctx golik.CloveContext) (persistance.Handler, error) {
	return &memDBHandler{
		db: db.db,
		table: db.Table,
		index: db.Index,
		behavior: db.createBehavior(),
	}, nil
}

