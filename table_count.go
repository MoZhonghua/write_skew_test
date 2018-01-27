package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/lib/pq"
)

var init_int_tables = []string{"int_table0", "int_table2", "int_table4"}
var all_int_tables = []string{"int_table", "int_table0", "int_table2", "int_table4", "int_table1", "int_table6"}

func tabnameToInt(s string) int {
	var id int
	fmt.Sscanf(s, "int_table%d", &id)
	return id
}

func tabCountSetupDB(db *sql.DB) {
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

func tabCountABeforeB() {
	db := openDatabase()
	defer db.Close()

	tabCountSetupDB(db)

	txnB, err := db.BeginTx(context.Background(), opts)
	tabCountRunTxnB(txnB)
	err = txnB.Commit()
	if err != nil {
		log.Fatal(err)
	}

	txnA, err := db.BeginTx(context.Background(), opts)
	tabCountRunTxnA(txnA)

	err = txnA.Commit()
	if err != nil {
		log.Fatal(err)
	}
	tabCountPrintData(db)
}

func tabCountBBeforeA() {
	db := openDatabase()
	defer db.Close()

	tabCountSetupDB(db)
	// tabCountPrintData(db)

	txnA, err := db.BeginTx(context.Background(), opts)
	tabCountRunTxnA(txnA)

	err = txnA.Commit()
	if err != nil {
		log.Fatal(err)
	}

	// tabCountPrintData(db)

	txnB, err := db.BeginTx(context.Background(), opts)
	tabCountRunTxnB(txnB)
	err = txnB.Commit()
	if err != nil {
		log.Fatal(err)
	}
	tabCountPrintData(db)
}

func tabCountConcurrent() {
	db := openDatabase()
	defer db.Close()

	tabCountSetupDB(db)

	txnA, err := db.BeginTx(context.Background(), opts)
	txnB, err := db.BeginTx(context.Background(), opts)
	tabCountRunTxnA(txnA)
	tabCountRunTxnB(txnB)

	err = txnA.Commit()
	if err != nil {
		log.Fatal(err)
	}

	err = txnB.Commit()
	if err != nil {
		log.Fatal(err)
	}
	tabCountPrintData(db)
}

func tableCountTest() {
	tabCountBBeforeA()
	tabCountABeforeB()
	tabCountConcurrent()
}

func tabCountPrintData(db *sql.DB) {
	log.Printf("===========print data========================")
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
	log.Printf("===========print data end========================")
}

func tabCountIterate(tx *sql.Tx) (evens, odds int) {
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

func tabCountRunTxnA(tx *sql.Tx) {
	_, err := tx.Exec("create table int_table1 (id int primary key)")
	if err != nil {
		log.Fatal(err)
	}

	evens, _ := tabCountIterate(tx)
	// log.Printf("%d evens, %d odds in database", evens, odds)

	_, err = tx.Exec(fmt.Sprintf("insert into sum (name, count) values ('%s', '%d')", "_evens", evens))
	if err != nil {
		log.Fatal(err)
	}
}

func tabCountRunTxnB(tx *sql.Tx) {
	_, err := tx.Exec("create table int_table6 (id int primary key)")
	if err != nil {
		log.Fatal(err)
	}

	_, odds := tabCountIterate(tx)
	// log.Printf("%d evens, %d odds in database", evens, odds)

	_, err = tx.Exec(fmt.Sprintf("insert into sum (name, count) values ('%s', '%d')", "_odds", odds))
	if err != nil {
		log.Fatal(err)
	}
}
