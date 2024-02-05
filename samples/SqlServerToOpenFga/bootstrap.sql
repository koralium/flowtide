CREATE DATABASE demo;
ALTER DATABASE demo SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);

CREATE TABLE demo.dbo.users (
    userkey int primary key,
    userid nvarchar(50)
);

CREATE TABLE demo.dbo.usergroups (
    userkey int,
    groupkey int
    primary key (userkey, groupkey)
);

CREATE TABLE demo.dbo.groups (
    groupkey int primary key,
    groupid nvarchar(50)
);

ALTER TABLE demo.dbo.users ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
ALTER TABLE demo.dbo.usergroups ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
ALTER TABLE demo.dbo.groups ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);

INSERT INTO demo.dbo.users (userkey, userid)
VALUES 
(1, 'user1'),
(2, 'user2'),
(3, 'user3'),
(4, 'user4'),
(5, 'user5');

INSERT INTO demo.dbo.groups (groupkey, groupid)
VALUES 
(1, 'group1'),
(2, 'group2'),
(3, 'group3'),
(4, 'group4'),
(5, 'group5');

INSERT INTO demo.dbo.usergroups (userkey, groupkey)
VALUES 
(1, 1),
(2, 1),
(2, 2),
(3, 2),
(4, 4),
(5, 5);

