package memdb

import (
	"fmt"
	"reflect"

	"github.com/hashicorp/go-memdb"
	mdb "github.com/hashicorp/go-memdb"
	"github.com/ioswarm/golik/db"
)

func indexTypeOf(ftype reflect.Type, fieldname string) mdb.Indexer {
	switch ftype.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return &mdb.IntFieldIndex{Field: fieldname}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &mdb.UintFieldIndex{Field: fieldname}
	case reflect.Bool:
		return &mdb.BoolFieldIndex{Field: fieldname}
	case reflect.String:
		return &mdb.StringFieldIndex{Field: fieldname}
	case reflect.Map:
		if ftype.Key().Kind() == reflect.String && ftype.Elem().Kind() == reflect.String {
			return &mdb.StringMapFieldIndex{Field: fieldname}
		}
	case reflect.Slice:
		if ftype.Elem().Kind() == reflect.String {
			return &mdb.StringSliceFieldIndex{Field: fieldname}
		}
	}
	return nil
}

func CreateTableSchema(ttype reflect.Type, indexField string) (*mdb.TableSchema, error) {
	if ttype.Kind() != reflect.Struct {
		return nil, fmt.Errorf("Given type must be a struct got %v", ttype.Kind())
	}

	tblname := db.CamelCase(ttype.Name())

	res := &mdb.TableSchema{Name: tblname, Indexes: make(map[string]*mdb.IndexSchema)}

	for i := 0; i < ttype.NumField(); i++ {
		fld := ttype.Field(i)
		fname := fld.Name 
		funame := []rune(fname)
		if funame[0] >= 65 && funame[0] <= 90 {
			ccfname := db.CamelCase(fname)
			if ccfname == indexField {
				idx := indexTypeOf(fld.Type, fname)
				if idx == nil {
					return nil, fmt.Errorf("No indexer for type %v present", fld.Type)
				}
				res.Indexes["id"] = &mdb.IndexSchema{
					Name: "id",
					Unique: true,
					Indexer: idx,
				}
			}
		}
	}

	if len(res.Indexes) == 0 {
		return nil, fmt.Errorf("Index-Field %v not found in %v", indexField, ttype.Name())
	}

	return res, nil
}

func CreateSingleDBSchema(ttype reflect.Type, indexField string) (*mdb.DBSchema, error) {
	tbl, err := CreateTableSchema(ttype, indexField)
	if err != nil {
		return nil, err
	}

	return &mdb.DBSchema{
		Tables: map[string]*mdb.TableSchema{
			tbl.Name: tbl,
		},
	}, nil
}

func firstTable(schema *memdb.DBSchema) *memdb.TableSchema {
	for key := range schema.Tables {
		return schema.Tables[key]
	}

	return nil
}

func firstUniquIndex(table *memdb.TableSchema) *memdb.IndexSchema {
	for key := range table.Indexes {
		idx := table.Indexes[key]
		if idx.Unique {
			return idx
		}
	}

	return nil
}