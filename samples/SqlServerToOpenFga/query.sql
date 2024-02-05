INSERT INTO openfga
SELECT 
  'user' AS user_type,
  userId as user_id,
  'member' as relation,
  'group' as object_type,
  groupId as object_id
FROM 
  groupMembers