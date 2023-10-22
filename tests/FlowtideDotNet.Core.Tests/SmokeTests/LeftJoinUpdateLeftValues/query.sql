SELECT l.Orderkey, l.Linenumber, s.Cost FROM lineitems l
LEFT JOIN Shipmodes s
ON l.Shipmode = s.Mode