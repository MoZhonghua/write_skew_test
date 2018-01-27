package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

var dbuser string
var dbname string

func main() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	flag.StringVar(&dbuser, "user", "mzh", "postgresql user")
	flag.StringVar(&dbname, "db", "write_skew_test", "postgresql database name")
	flag.Parse()

	tableCountTest()
	log.Printf("--------------row count test ----------------")
	rowCountTest()
}

func openDatabase() *sql.DB {
	connStr := fmt.Sprintf("user=%s dbname=%s sslmode=disable", dbuser, dbname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

var opts = &sql.TxOptions{}

func init() {
	opts.Isolation = sql.LevelSerializable
	// opts.Isolation = sql.LevelReadCommitted
}

