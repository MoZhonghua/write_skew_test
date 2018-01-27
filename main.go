package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	_ "github.com/lib/pq"
)

var dbuser string
var dbname string

var init_int_tables = []string{"int_table0", "int_table2", "int_table4"}
var all_int_tables = []string{"int_table0", "int_table2", "int_table4", "int_table1", "int_table6"}

func tabnameToInt(s string) int {
	var id int
	fmt.Sscanf(s, "int_table%d", &id)
	return id
}

func initDatabase(db *sql.DB) {
	for _, t := range all_int_tables {
		db.Exec(fmt.Sprintf("drop table %v", t))
	}

	for _, t := range init_int_tables {
		_, err := db.Exec(fmt.Sprintf("create table %v (id int primary key)", t))
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err := db.Exec("create table if not exists sum (name varchar primary key, count int)")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("delete from sum")
	if err != nil {
		log.Fatal(err)
	}
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
}

func runTxnBbeforeA() {
	db := openDatabase()
	defer db.Close()

	initDatabase(db)

	txnB, err := db.BeginTx(context.Background(), opts)
	runTxnB(txnB)
	err = txnB.Commit()
	if err != nil {
		log.Fatal(err)
	}

	txnA, err := db.BeginTx(context.Background(), opts)
	runTxnA(txnA)

	err = txnA.Commit()
	if err != nil {
		log.Fatal(err)
	}
	printData(db)
}

func runTxnAbeforeB() {
	db := openDatabase()
	defer db.Close()

	initDatabase(db)

	txnA, err := db.BeginTx(context.Background(), opts)
	runTxnA(txnA)

	err = txnA.Commit()
	if err != nil {
		log.Fatal(err)
	}

	txnB, err := db.BeginTx(context.Background(), opts)
	runTxnB(txnB)
	err = txnB.Commit()
	if err != nil {
		log.Fatal(err)
	}
	printData(db)
}

func runTxnConcurrent() {
	db := openDatabase()
	defer db.Close()

	initDatabase(db)

	txnA, err := db.BeginTx(context.Background(), opts)
	txnB, err := db.BeginTx(context.Background(), opts)
	runTxnA(txnA)
	runTxnB(txnB)

	err = txnA.Commit()
	if err != nil {
		log.Fatal(err)
	}

	err = txnB.Commit()
	if err != nil {
		log.Fatal(err)
	}
	printData(db)
}

func main() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	flag.StringVar(&dbuser, "user", "mzh", "postgresql user")
	flag.StringVar(&dbname, "db", "write_skew_test", "postgresql database name")
	flag.Parse()

	runTxnAbeforeB()
	runTxnBbeforeA()
	runTxnConcurrent()
}

func printData(db *sql.DB) {
	log.Printf("=========================================")
	for _, t := range all_int_tables {
		_, err := db.Query(fmt.Sprintf("select * from %s", t))
		if err != nil {
			continue
		}
		log.Printf("%s", t)
	}

	rows2, err := db.Query("select * from sum")
	if err != nil {
		log.Fatal(err)
	}

	for rows2.Next() {
		var name string
		var count int

		rows2.Scan(&name, &count)
		log.Printf("sum: %v =  %d", name, count)
	}

	rows2.Close()
}

func iterate(tx *sql.Tx) (evens, odds int) {
	log.Printf("=========================================")
	rows, err := tx.Query("select tablename from pg_catalog.pg_tables")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var t string
		rows.Scan(&t)
		if !strings.HasPrefix(t, "int_table") {
			continue
		}
		id := tabnameToInt(t)
		if id%2 == 0 {
			evens += 1
		} else {
			odds += 1
		}
	}
	return
}

func runTxnA(tx *sql.Tx) {
	_, err := tx.Exec("create table int_table1 (id int primary key)")
	if err != nil {
		log.Fatal(err)
	}

	evens, odds := iterate(tx)
	log.Printf("%d evens, %d odds in database", evens, odds)

	_, err = tx.Exec(fmt.Sprintf("insert into sum (name, count) values ('%s', '%d')", "_evens", evens))
	if err != nil {
		log.Fatal(err)
	}
}

func runTxnB(tx *sql.Tx) {
	_, err := tx.Exec("create table int_table6 (id int primary key)")
	if err != nil {
		log.Fatal(err)
	}

	_, odds := iterate(tx)
	// log.Printf("%d evens, %d odds in database", evens, odds)

	_, err = tx.Exec(fmt.Sprintf("insert into sum (name, count) values ('%s', '%d')", "_odds", odds))
	if err != nil {
		log.Fatal(err)
	}
}
