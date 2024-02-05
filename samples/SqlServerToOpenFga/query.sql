INSERT INTO openfga
SELECT 
  'user' AS user_type,
  u.userId as user_id,
  'member' as relation,
  'group' as object_type,
  g.groupId as object_id
FROM demo.dbo.usergroups ug
INNER JOIN demo.dbo.users u ON ug.userkey = u.userkey
INNER JOIN demo.dbo.groups g ON ug.groupkey = g.groupkey
 