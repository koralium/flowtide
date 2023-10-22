SELECT l.Orderkey, l.Linenumber, l.Quantity, o.Orderstatus, o.Custkey FROM lineitems l
INNER JOIN Orders o
ON l.Orderkey = o.Orderkey