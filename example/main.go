package main

import (
	"context"
	"log"
	"os"
	"reflect"

	//"github.com/hashicorp/go-memdb"
	"github.com/ioswarm/golik"
	mem "github.com/ioswarm/golik-memdb"

	//"github.com/ioswarm/golik/filter"
	"github.com/ioswarm/golik/persistance"
)

type Person struct {
	Email string
	Name string
	Age int
}

func main() {

	sys, err := golik.NewSystem("memdb-example")
	if err != nil {
		log.Panic(err)
	}

	/*ref, err := sys.Execute(persistance.NewConnectionPool(&mem.MemDBPoolSettings{
		Table: "person",
		Index: "id",
		Schema: &memdb.DBSchema{
			Tables: map[string]*memdb.TableSchema{
				"person":  {
					Name: "person",
					Indexes: map[string]*memdb.IndexSchema{
						"id": {
							Name:    "id",
							Unique:  true,
							Indexer: &memdb.StringFieldIndex{Field: "Email"},
						},
					},
				},
			},
		},
	}))*/

	stt, err := mem.NewMemDBPoolSettingsOf(reflect.TypeOf(Person{}), "email")
	if err != nil {
		log.Panic(err)
	}

	ref, err := sys.Execute(persistance.NewConnectionPool(stt))
	if err != nil {
		log.Panic(err)
	}

	log.Println("Create result with:", <- ref.Request(context.Background(), &persistance.Create{Entity: &Person{Email: "john@doe.com", Name:"John Doe", Age: 49}}))

	res, err := ref.RequestFunc(context.Background(), &persistance.Get{Id: "john@doe.com"})
	if err != nil {
		log.Panic(err)
	}

	log.Println("Got:", res)

	os.Exit(<-sys.Terminated())
}