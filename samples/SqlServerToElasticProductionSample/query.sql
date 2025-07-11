INSERT INTO docs
SELECT 
	orderkey as _id, orderdate, firstname, lastname
FROM test.dbo.orders o
LEFT JOIN test.dbo.users u
ON o.userkey = u.userkey;