---
sidebar_position: 7
---

# Sharepoint Connector

The *Sharepoint Connector* at this time only supports a sink where data can be stored into a sharepoint list.

## List Sink

The *List Sink* saves result from a stream into a sharepoint list.

To use it, add the following nuget:

* FlowtideDotNet.Connector.Sharepoint

Add it to the *ConnectorManager*:

```csharp
connectorManager.AddSharepointListSink("{regexPattern}", (writeRelation) => new SharepointSinkOptions()
{
    // Set the primary key in sharepoint, it is the name of the column in sharepoint
    PrimaryKeyColumnNames = new List<string> { "primaryKeyColumnName" }
    SharepointUrl = "{your company name}.sharepoint.com",
    Site = "{Name of sharepoint site}"
    TokenCredential = yourAzureTokenCredential
})
```

### Supported column types

The following column types are supported:

* **Text** - Data in should be of data type string
* **PersonOrGroup** - Data in should be of type string and contain a UPN such as email.
* **Boolean** - Data type should be a boolean (true/false).
* **DateTime** - Data in should be a unix timestamp.
* **Multi line** - Same as text.
* **Choice** - Data in should be of data type string and contain one of the choices.
* **Number** - Data in should be an integer or a float.
* **Currency** - Data in should be an integer or a float.

### Options

The following options can be set on the sink:

* **PrimaryKeyColumnNames** - Required, a list of sharepoint column names that make up the primary key.
* **SharepointUrl** Required, url to the sharepoint tenant, should not contain 'https://'.
* **Site** - Required, name to the site where the list is.
* **TokenCredential** - Required, the token credential that is used to authenticate against sharepoint.
* **DisableDelete** - Disables row deletion.
* **ThrowOnPersonOrGroupNotFound** - Instead of returning null when a person is not found, throw an exception.
* **PreprocessRow** - Action that is called on each upsert, allows addition of metadata columns.