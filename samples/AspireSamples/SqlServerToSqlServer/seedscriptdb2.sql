IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'test')
BEGIN
  CREATE DATABASE test;
END;
GO

USE test;
GO

-- Create the Contacts table
IF OBJECT_ID(N'destinationtable', N'U') IS NULL
BEGIN
    CREATE TABLE destinationtable
    (
        OrderKey 	 INT PRIMARY KEY NOT NULL,
        OrderDate    DATETIME2 NOT NULL,
        FirstName   VARCHAR(255) NULL,
        LastName  VARCHAR(255) NULL,
    );
END;
GO


