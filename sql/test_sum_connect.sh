#!/bin/bash

SKDB=../build/skdb
DB=/tmp/test.db

rm -f /tmp/foo $DB

$SKDB --init $DB
echo "create table t1 (a INTEGER); insert into t1 values(22); create view v1 as select sum(a) from t1;" | $SKDB --data $DB

for i in {1..1}; do
    rm -f "/tmp/foo$i"
    $SKDB --connect v1 --data $DB --file "/tmp/foo$i"
done

for j in {1..10}; do
    (echo "begin transaction;"; for i in {1..10000}; do echo "insert into t1 values($i);"; done; echo "commit;"; ) | $SKDB --data $DB &
done;
wait