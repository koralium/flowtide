USE master;
CREATE DATABASE [demo];

GO

USE demo;
CREATE TABLE demo.dbo.users (
    userkey int primary key,
    userid uniqueidentifier
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

ALTER DATABASE [demo] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);

ALTER TABLE demo.dbo.users ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
ALTER TABLE demo.dbo.usergroups ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
ALTER TABLE demo.dbo.groups ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);

INSERT INTO demo.dbo.users (userkey, userid)
VALUES 
(1, '57f20bbe-3a17-45a7-bacc-614d89bde120'),
(2, '6c2aa4a7-1cfa-4fb3-84d2-45af79a9ae65'),
(3, 'cc1aa2df-9e85-487b-96b2-4bf95d3d291c'),
(4, '5b47faac-d36d-4c73-9685-6d3beda97760'),
(5, 'c6bec31e-03fc-4202-90fa-8fcfe024ae0c');

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

