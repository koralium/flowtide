# SQL Server To SQL Server Sample

This sample shows a stream joining two tables from one sql server and inserting them into another sql server.
The tables do not have to be on the same sql server, or dont even need to both be a SQL Server table, but for the convenience of the demo
they are located on the same server.

This sample first generates an initial 100 000 users and 100 000 orders as an initial data batch.
It creates a stream that joins these two tables together. After that it inserts 1000 users and 1000 orders using entity framework as quickly
as it can with one thread to generate a changing stream.

## Starting the sample

The easiest way to start the sample is to run the AspireSamples project, and select 'SqlServer-To-SqlServer' in the choices.

To look at the stream in the web UI, click on the endpoint for the project named "stream".

To investigate how the data looks in both SQL Servers, there is a 'sqlpad' resource with an endpoint.
Click on that endpoint to run queries against the databases.
