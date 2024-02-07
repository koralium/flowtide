USE master;
CREATE DATABASE [demo];

GO

USE demo;
CREATE TABLE demo.dbo.docs (
    docid nvarchar(50) primary key,
    [name] nvarchar(50)
);

ALTER DATABASE [demo] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);

ALTER TABLE demo.dbo.docs ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);

INSERT INTO demo.dbo.docs (docid, [name])
VALUES
('1', 'doc1'),
('2', 'doc2');
