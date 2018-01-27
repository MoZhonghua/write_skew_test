package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func initDatabase(db *sql.DB) {
	_, err := db.Exec("create table if not exists int_table (id int primary key)")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("delete from int_table")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("insert into int_table(id) values (0)")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("insert into int_table(id) values (2)")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("insert into int_table(id) values (4)")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("create table if not exists sum (name varchar primary key, count int)")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("delete from sum")
	if err != nil {
		log.Fatal(err)
	}
}

var opts = &sql.TxOptions{}

func init() {
	opts.Isolation = sql.LevelSerializable
}

func runTxnBbeforeA() {
	connStr := "user=mzh dbname=write_skew_test sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err != nil {
		log.Fatal(err)
	}

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
	connStr := "user=mzh dbname=write_skew_test sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err != nil {
		log.Fatal(err)
	}

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
	connStr := "user=mzh dbname=write_skew_test sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err != nil {
		log.Fatal(err)
	}

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
	runTxnAbeforeB()
	runTxnBbeforeA()
	runTxnConcurrent()
}

func printData(db *sql.DB) {
	log.Printf("=========================================")
	rows, err := db.Query("select * from int_table")
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var id int
		rows.Scan(&id)
		log.Printf("id: %d", id)
	}

	rows.Close()

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

func iterate(rows *sql.Rows) (evens, odds int) {
	for rows.Next() {
		var id int
		rows.Scan(&id)
		if id%2 == 0 {
			evens += 1
		} else {
			odds += 1
		}
	}
	return
}

func runTxnA(tx *sql.Tx) {
	_, err := tx.Exec("insert into int_table(id) values (1)")
	if err != nil {
		log.Fatal(err)
	}

	rows, err := tx.Query("select * from int_table")
	if err != nil {
		log.Fatal(err)
	}

	evens, _ := iterate(rows)
	// log.Printf("%d evens, %d odds in database", evens, odds)
	rows.Close()

	_, err = tx.Exec(fmt.Sprintf("insert into sum (name, count) values ('%s', '%d')", "_evens", evens))
	if err != nil {
		log.Fatal(err)
	}
}

func runTxnB(tx *sql.Tx) {
	_, err := tx.Exec("insert into int_table(id) values (6)")
	if err != nil {
		log.Fatal(err)
	}

	rows, err := tx.Query("select * from int_table")
	if err != nil {
		log.Fatal(err)
	}

	_, odds := iterate(rows)
	// log.Printf("%d evens, %d odds in database", evens, odds)
	rows.Close()

	_, err = tx.Exec(fmt.Sprintf("insert into sum (name, count) values ('%s', '%d')", "_odds", odds))
	if err != nil {
		log.Fatal(err)
	}
}
