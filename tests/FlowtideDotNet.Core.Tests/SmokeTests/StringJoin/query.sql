SELECT l.Orderkey, l.Linenumber, s.Cost FROM lineitems l
INNER JOIN Shipmodes s
ON l.Shipmode = s.Mode