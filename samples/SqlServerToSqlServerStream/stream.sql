INSERT INTO db2.test.dbo.destinationtable
SELECT 
	orderkey, orderdate, firstname, lastname
FROM db1.test.dbo.orders o
LEFT JOIN db1.test.dbo.users u
ON o.userkey = u.userkey;