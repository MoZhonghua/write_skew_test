#! /bin/bash
psql -d write_skew_test -a -f ./init.sql
echo "Database initialized."
sleep 1

psql -d write_skew_test -a -f ./txn_b.sql  > /tmp/txn_b.log 2>&1 &
psql -d write_skew_test -a -f ./txn_a.sql  > /tmp/txn_a.log 2>&1 &

for job in `jobs -p`
do
    wait $job
done

echo "---------------TXN A OUPTPUT------------------"
cat /tmp/txn_a.log
echo "---------------TXN B OUPTPUT------------------"
cat /tmp/txn_b.log

echo "---------------DUMP OUPTPUT------------------"
psql -d write_skew_test -a -f ./dump.sql

