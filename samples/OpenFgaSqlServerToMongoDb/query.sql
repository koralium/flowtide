CREATE VIEW docpermissions AS
SELECT
  object_id,
  list_agg(user_type || ':' || user_id) AS permissions
FROM permissionview
GROUP BY object_id;

INSERT INTO outputdata
SELECT docid, [name], permissions
FROM demo.dbo.docs
LEFT JOIN docpermissions ON docid = object_id;