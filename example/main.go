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
	"github.com/ioswarm/golik/db"
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

	mdb, err := mem.MemDB(sys)
	if err != nil {
		log.Panic(err)
	}
	
	pool, err := mdb.CreateConnectionPool(&db.ConnectionPoolSettings{
		Name: "person",
		Type: reflect.TypeOf(Person{}),
		PoolSize: 10,
	})
	if err != nil {
		log.Panic(err)
	}

	log.Println("Create result with:", <- pool.Request(context.Background(), db.Create(&Person{Email: "john@doe.com", Name:"John Doe", Age: 49})))

	res, err := pool.RequestFunc(context.Background(), db.Get("john@doe.com"))
	if err != nil {
		log.Panic(err)
	}

	log.Println("Got:", res)

	os.Exit(<-sys.Terminated())
}