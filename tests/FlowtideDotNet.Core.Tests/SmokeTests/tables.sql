CREATE TABLE lineitems (
  Orderkey bigint not null,
  Linenumber int not null,
  Partkey bigint,
  Suppkey bigint,
  Quantity float,
  Extendedprice float,
  Discount float,
  Tax float,
  Returnflag varchar,
  Linestatus varchar,
  Shipdate date,
  Commitdate date,
  Receiptdate date,
  Shipinstruct varchar,
  Shipmode varchar,
  Comment varchar
);

CREATE TABLE orders (
  Orderkey bigint not null,
  Custkey bigint,
  Orderstatus varchar,
  Totalprice float,
  Orderdate date,
  Orderpriority varchar,
  Clerk varchar,
  Shippriority int,
  Comment varchar
);

CREATE TABLE shipmodes (
  ShipmodeKey int not null,
  Mode varchar,
  Cost float
);