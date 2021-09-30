package main

import (
	"encoding/json"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/sirupsen/logrus"
)

type EventHandler struct {
	canal.DummyEventHandler
}
type InsertEvent struct {
	Table  string
	Schema string
	Data   map[string]interface{}
}

func (h *EventHandler) OnRow(e *canal.RowsEvent) error {
	switch e.Action {
	case "insert":
		OnInsert(e)
	default:
		logrus.Errorf("unknown event type: %s", e.Action)
	}
	return nil
}
func OnInsert(e *canal.RowsEvent) {
	table := e.Table.Name
	schema := e.Table.Schema
	for _, row := range e.Rows {
		var ie InsertEvent
		ie.Table = table
		ie.Schema = schema
		ie.Data = make(map[string]interface{})
		for k, c := range e.Table.Columns {
			ie.Data[c.Name] = row[k]
		}
		b, err := json.Marshal(ie)
		if err != nil {
			logrus.Error(err)
		}
		err = Send(b)
		if err != nil {
			logrus.Error(err)
		}
	}
}

//TODO:send into MessageQueue
func Send(b []byte) error {
	logrus.Infof("insert: %s", b)
	return nil
}
func main() {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = "127.0.0.1:3306"
	cfg.User = "canal"
	cfg.Password = "canal"

	c, err := canal.NewCanal(cfg)
	if err != nil {
		logrus.Fatal(err)
	}
	// Register a handler to handle RowsEvent
	c.SetEventHandler(&EventHandler{})
	coords, err := c.GetMasterPos()
	if err != nil {
		logrus.Fatal(err)
	}
	// Start canal
	c.RunFrom(coords)
}
