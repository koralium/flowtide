IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'test')
BEGIN
  CREATE DATABASE test;
  ALTER DATABASE [test] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
END;
GO

USE test;
GO

-- Create the Contacts table
IF OBJECT_ID(N'sourcetable', N'U') IS NULL
BEGIN
    CREATE TABLE sourcetable
    (
        Id        INT PRIMARY KEY IDENTITY(1,1) ,
        FirstName VARCHAR(255) NOT NULL,
        LastName  VARCHAR(255) NOT NULL,
        Email     VARCHAR(255) NULL,
    );
    ALTER TABLE test.dbo.sourcetable ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
END;
GO

IF (SELECT COUNT(*) FROM sourcetable) = 0
BEGIN
    INSERT INTO sourcetable (FirstName, LastName, Email)
    VALUES
        ('John', 'Doe', 'john.doe@example.com'),
        ('Jane', 'Doe', 'jane.doe@example.com'),
        ('tester', 'test', 'tester.test@testing.com');
END;
GO


