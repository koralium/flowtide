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
        LastName  VARCHAR(255) PRIMARY KEY NOT NULL,
        FirstNames     VARCHAR(MAX) NULL,
    );
END;
GO


