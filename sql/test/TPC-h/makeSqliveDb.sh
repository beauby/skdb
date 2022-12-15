#!/bin/bash

TPCHDB='/home/julienv/tnt-tpch/TPC-H.db'

rm -f /tmp/test.db

skdb --init /tmp/test.db

echo "
CREATE TABLE CUSTOMER (
  c_custkey    INTEGER PRIMARY KEY,
  c_name       TEXT,
  c_address    TEXT,
  c_nationkey  INTEGER,
  c_phone      TEXT,
  c_acctbal    FLOAT,
  c_mktsegment TEXT,
  c_comment    TEXT
);
" | skdb --data /tmp/test.db

echo "
CREATE TABLE LINEITEM (
  l_orderkey      INTEGER,
  l_partkey       INTEGER,
  l_suppkey       INTEGER,
  l_linenumber    INTEGER,
  l_quantity      INTEGER,
  l_extendedprice FLOAT,
  l_discount      FLOAT,
  l_tax           FLOAT,
  l_returnflag    TEXT,
  l_linestatus    TEXT,
  l_shipdate      TEXT,
  l_commitdate    TEXT,
  l_receiptdate   TEXT,
  l_shipinstruct  TEXT,
  l_shipmode      TEXT,
  l_comment       TEXT
);
" | skdb --data /tmp/test.db

echo "
CREATE TABLE NATION (
  n_nationkey INTEGER PRIMARY KEY,
  n_name      TEXT,
  n_regionkey INTEGER,
  n_comment   TEXT
);
" | skdb --data /tmp/test.db

echo "
CREATE TABLE ORDERS (
  o_orderkey      INTEGER PRIMARY KEY,
  o_custkey       INTEGER,
  o_orderstatus   TEXT,
  o_totalprice    FLOAT,
  o_orderdate     TEXT,
  o_orderpriority TEXT,
  o_clerk         TEXT,
  o_shippriority  INTEGER,
  o_comment       TEXT
);
" | skdb --data /tmp/test.db

echo "
CREATE TABLE PART (
  p_partkey     INTEGER PRIMARY KEY,
  p_name        TEXT,
  p_mfgr        TEXT,
  p_brand       TEXT,
  p_type        TEXT,
  p_size        INTEGER,
  p_container   TEXT,
  p_retailprice FLOAT,
  p_comment     TEXT
);
" | skdb --data /tmp/test.db

echo "
CREATE TABLE PARTSUPP (
  ps_partkey    INTEGER,
  ps_suppkey    INTEGER,
  ps_availqty   INTEGER,
  ps_supplycost FLOAT,
  ps_comment    TEXT
);
" | skdb --data /tmp/test.db

echo "
CREATE TABLE REGION (
  r_regionkey INTEGER PRIMARY KEY,
  r_name      TEXT,
  r_comment   TEXT
);
" | skdb --data /tmp/test.db

echo "
CREATE TABLE SUPPLIER (
  s_suppkey   INTEGER PRIMARY KEY,
  s_name      TEXT,
  s_address   TEXT,
  s_nationkey INTEGER,
  s_phone     TEXT,
  s_acctbal   FLOAT,
  s_comment   TEXT
);
" | skdb --data /tmp/test.db

echo "select * from customer;" | sqlite3 $TPCHDB -csv | skdb --load-csv CUSTOMER --data /tmp/test.db 
echo "select * from lineitem;" | sqlite3 $TPCHDB -csv | head -n 500000 | skdb --load-csv LINEITEM --data /tmp/test.db
echo "select * from nation;" | sqlite3 $TPCHDB -csv | skdb --load-csv NATION --data /tmp/test.db 
echo "select * from orders;" | sqlite3 $TPCHDB -csv | skdb --load-csv ORDERS --data /tmp/test.db 
echo "select * from part;" | sqlite3 $TPCHDB -csv | skdb --load-csv PART --data /tmp/test.db
echo "select * from partsupp;" | sqlite3 $TPCHDB -csv | skdb --load-csv PARTSUPP --data /tmp/test.db
echo "select * from region;" | sqlite3 $TPCHDB -csv | skdb --load-csv REGION --data /tmp/test.db
echo "select * from supplier;" | sqlite3 $TPCHDB -csv | skdb --load-csv SUPPLIER --data /tmp/test.db

skdb --data /tmp/test.db --compact