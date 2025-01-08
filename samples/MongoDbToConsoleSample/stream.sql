INSERT INTO console
SELECT o._doc.OrderKey, u._doc.FirstName, u._doc.LastName FROM test.orders o
INNER JOIN test.users u
ON o._doc.UserKey = u._doc.UserKey;