package memdb

import (
	"github.com/hashicorp/go-memdb"
	"github.com/ioswarm/golik"
	"github.com/ioswarm/golik/filter"
	"github.com/ioswarm/golik/db"
)

func defaultHandlerCreation(database *memdb.MemDB, table string, index string, behavior interface{}) db.HandlerCreation {
	return func(ctx golik.CloveContext) (db.Handler, error) {
		return NewMemDBHandler(database, table, index, behavior), nil
	}
}

func NewMemDBHandler(db *memdb.MemDB, table string, index string, behavior interface{}) db.Handler {
	return &memDBHandler{
		db:       db,
		table:    table,
		index:    index,
		behavior: behavior,
	}
}

type memDBHandler struct {
	db       *memdb.MemDB
	table    string
	index    string
	behavior interface{}
}

func (hdl *memDBHandler) Filter(ctx golik.CloveContext, flt *filter.Filter) (*filter.Result, error) {
	cond, err := flt.Condition()
	if err != nil {
		return nil, err
	}

	txn := hdl.db.Txn(false)
	defer txn.Abort()
	slice := make([]interface{}, 0)

	it, err := txn.Get(hdl.table, hdl.index)
	if err != nil {
		return nil, err
	}

	for data := it.Next(); data != nil; data = it.Next() {
		if cond.Check(data) {
			slice = append(slice, data)
		}
	}

	cnt := len(slice)
	if flt.From == 0 && flt.Size > 0 {
		if flt.Size < cnt {
			slice = slice[:flt.Size]
		}
	}
	if flt.From > 0 {
		if flt.From > cnt {
			slice = slice[cnt:cnt]
		} else {
			if flt.Size > 0 {
				to := flt.From+flt.Size
				if to > cnt {
					slice = slice[flt.From:cnt]
				} else {
					slice = slice[flt.From:to]
				}
			} else {
				slice = slice[flt.From:]
			}
		}
	}
	
	return &filter.Result{
		From: flt.From,
		Size: flt.Size,
		Count: cnt,
		Result: slice,
	}, nil
}

func (hdl *memDBHandler) Create(ctx golik.CloveContext, cmd *db.CreateCommand) error {
	if cmd != nil && cmd.Entity != nil {
		txn := hdl.db.Txn(true)
		if err := txn.Insert(hdl.table, cmd.Entity); err != nil {
			txn.Abort()
			return err
		}

		txn.Commit()
	}

	return nil
}

func (hdl *memDBHandler) Read(ctx golik.CloveContext, cmd *db.GetCommand) (interface{}, error) {
	if cmd != nil && cmd.Id != nil {
		txn := hdl.db.Txn(false)
		defer txn.Abort()
		
		data, err := txn.First(hdl.table, hdl.index, cmd.Id)
		if err != nil {
			return nil, err
		}
		
		return data, nil
	}
	return nil, nil
}

func (hdl *memDBHandler) Update(ctx golik.CloveContext, cmd *db.UpdateCommand) error {
	if cmd != nil {
		return hdl.Create(ctx, &db.CreateCommand{Entity: cmd.Entity})
	}
	return nil
}

func (hdl *memDBHandler) Delete(ctx golik.CloveContext, cmd *db.DeleteCommand) (interface{}, error) {
	if cmd != nil {
		data, err := hdl.Read(ctx, &db.GetCommand{Id: cmd.Id})
		if err != nil { 
			return nil, err
		}
		txn := hdl.db.Txn(true)
		defer txn.Commit()

		if err := txn.Delete(hdl.table, data); err != nil {
			return nil, err
		}
		return data, nil
	}
	return nil, nil
}

func (hdl *memDBHandler) OrElse(ctx golik.CloveContext, msg golik.Message) {
	if hdl.behavior != nil {
		golik.CallBehavior(ctx, msg, hdl.behavior)
	}
}
