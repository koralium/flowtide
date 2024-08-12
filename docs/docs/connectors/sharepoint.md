---
sidebar_position: 7
---

# Sharepoint Connector

The *Sharepoint Connector* at this time only supports a sink where data can be stored into a sharepoint list.

To use it, add the following nuget:

* FlowtideDotNet.Connector.Sharepoint

## List Sink

The *List Sink* saves result from a stream into a sharepoint list.

To use it, add it to the *ConnectorManager*:

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
* **SharepointUrl** - Required, url to the sharepoint tenant, should not contain 'https://'.
* **Site** - Required, name to the site where the list is.
* **TokenCredential** - Required, the token credential that is used to authenticate against sharepoint.
* **DisableDelete** - Disables row deletion.
* **ThrowOnPersonOrGroupNotFound** - Instead of returning null when a person is not found, throw an exception.
* **PreprocessRow** - Action that is called on each upsert, allows addition of metadata columns.

## List Source

The *List Source* allow a stream to read data and changes from a sharepoint list.

The *List Source* is added to the *ConnectorManager* as follows:

```csharp
connectorManager.AddSharepointSource(new SharepointSourceOptions()
{
    SharepointUrl = "{your company name}.sharepoint.com",
    Site = "{Name of sharepoint site}",
    TokenCredential = yourAzureTokenCredential
}, "{optional prefix}");
```

The following column types are supported:

* **Text**
* **Lookup**
* **PersonOrGroup**
* **Boolean** 
* **DateTime** 

More types will be added in the future. Please create a github issue for the type you are missing.

### Special columns

There are some special column names that allows you to fetch data:

* **ID** - The sharepoint row identifier.
* **_fields** - Contains all columns in a map. Useful if one wants the stream to dynamically handle the addition of new columns

The data in the *_fields* column has the following structure:

```json
{
  "{columnName1}": {
    "value": "{columnValue}",
    "description": "{column description}",
    "type": "{columnType}"
  },
  "{columnName2}": {
    ...
  }
}
```

### Options

The following options can be set on the source:

* **SharepointUrl** - Required, url to the sharepoint tenant, should not contain 'https://'.
* **Site** - Required, name to the site where the list is.
* **TokenCredential** - Required, the token credential that is used to authenticate against sharepoint.

