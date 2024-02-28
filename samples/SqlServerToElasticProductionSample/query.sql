INSERT INTO docs
SELECT 
  docid as _id,
  docid, 
  name
FROM demo.dbo.docs;