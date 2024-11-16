INSERT INTO db2.test.dbo.destinationtable
SELECT 
	LastName, list_agg(FirstName) as FirstNames
FROM db1.test.dbo.sourcetable
GROUP BY LastName;