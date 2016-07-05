package main

import (
	"encoding/json"
	"flag"
	"log"

	"database/sql"
	"github.com/lib/pq"

	"github.com/elastic/go-lumber/server"
)

type StringTable struct {
	table string
	cache map[string]int
}

func logPanic(msg string, v ...interface{}) {
	log.Printf(msg, v)
	panic(msg)
}

func NewStringTable(table string) *StringTable {
	return &StringTable{
		table: table,
		cache: make(map[string]int),
	}
}

func (self *StringTable) lookup(db *sql.DB, val string) (id int) {
	if id, ok := self.cache[val]; ok {
		return id
	}

	err := db.QueryRow("select id from "+self.table+" where name=$1", val).Scan(&id)

	if err == sql.ErrNoRows {
		err := db.QueryRow(
			"insert into "+self.table+" (name) values ($1) returning id",
			val).Scan(&id)

		if nil != err {
			// could check that we didn't duplicate with another process here...
			logPanic("couldn't update name table", err)
		}
	} else if nil != err {
		logPanic("couldn't query name table", err)
	}

	self.cache[val] = id
	return
}

func main() {
	bind := flag.String("bind", ":27044", "[host]:port to listen on")
	flag.Parse()
	s, err := server.ListenAndServe(*bind)
	if nil != err {
		logPanic("couldn't listen for connections", err)
	}

	db, err := sql.Open("postgres", "postgres://localhost/logs")
	if nil != err {
		logPanic("couldn't connect to db", err)
	}
	defer db.Close()

	fileNameCache := NewStringTable("files")
	hostNameCache := NewStringTable("hosts")

	for batch := range s.ReceiveChan() {
		txn, err := db.Begin()
		if nil != err {
			logPanic("couldn't open transaction", err)
		}
		//		stmt, err := txn.Prepare("INSERT INTO logs " +
		//		"(event_timestamp, message, extra) VALUES ($1,$2,$3)")
		stmt, err := txn.Prepare(pq.CopyIn("logs", "event_timestamp", "file_id", "host_id", "message", "extra"))
		if nil != err {
			logPanic("couldn't prepare", err)
		}
		for _, item := range batch.Events {
			as_map := item.(map[string]interface{})
			event_timestamp := as_map["@timestamp"]
			delete(as_map, "@timestamp")
			event_message := as_map["message"]
			delete(as_map, "message")
			file_id := fileNameCache.lookup(db, as_map["source"].(string))
			delete(as_map, "source")
			beat_map := as_map["beat"].(map[string]interface{})
			host_id := hostNameCache.lookup(db, beat_map["name"].(string))
			delete(beat_map, "name")
			delete(beat_map, "hostname")

			remainder, err := json.Marshal(as_map)
			if nil != err {
				logPanic("couldn't marshal", err)
			}

			if _, err := stmt.Exec(
				event_timestamp,
				file_id,
				host_id,
				event_message,
				string(remainder)); nil != err {
				logPanic("couldn't exec insert", err)
			}
		}
		if _, err := stmt.Exec(); nil != err {
			logPanic("couldn't exec flush", err)
		}
		if err := stmt.Close(); nil != err {
			logPanic("couldn't close batch insert", err)
		}
		if err := txn.Commit(); nil != err {
			logPanic("couldn't commit transaction", err)
		}
		batch.ACK()
	}
}
