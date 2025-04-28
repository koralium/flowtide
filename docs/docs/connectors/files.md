---
sidebar_position: 11
---

# Files Connector

:::warning

The files connector is experimental and may be subject to change.

:::

The files connector allows a stream to read files from local disk or from a cloud solution into the stream. At this point it is only supported as data sources.

To use this connector, you need to install:

* FlowtideDotNet.Connector.Files

## CSV Files

The CSV File connector allows ingesting structured data from CSV files into a Flowtide stream. It’s ideal for batch-style file processing and can be configured to read static files or continuously monitor for new deltas.

### Adding the CSV File Source

Use the `AddCsvFileSource` method to register the connector as a source with a specific table name.


```csharp
connectorManager.AddCsvFileSource("my_csv_table", new CsvFileOptions
{
    FileStorage = Files.Of.LocalDisk("./csvdata"),
    CsvColumns = new List<string> { "id", "name", "email" },
    GetInitialFiles = async () => new[] { "data.csv" }
});
```

You can now query your CSV file directly:

```sql
SELECT * FROM my_csv_table
```

### Options

| Name                  | Description                                                                                 | Required | Default |
| --------------------- | ------------------------------------------------------------------------------------------- | -------- | ------- |
| FileStorage           | Storage backend to use (e.g., local disk, Azure Blob, etc.).                                | Yes      | -       |
| CsvColumns            | List of column names in the CSV files.                                                      | Yes      | -       |
| Delimiter             | Delimiter used in the CSV files.                                                            | No       | ','     |
| GetInitialFiles       | Function that returns the initial list of files to load.                                    | Yes      | -       |
| OutputSchema          | Optional schema for output columns and types. If not set, it is inferred from `CsvColumns`. | No       | null    |
| BeforeReadFile        | Hook to execute before reading each file. Allows custom state preperation.                  | No       | null    |
| BeforeBatch           | Hook before reading a batch of files. Useful for shared setup per batch.                    | No       | null    |
| ModifyRow             | Hook to modify row content before transformation.                                           | No       | null    |
| InitialWeightFunction | Optional weight function for initial files (used to mark deletes, default is 1).            | No       | null    |
| DeltaCsvColumns       | Used in delta mode. List of column names in the delta csv files.                            | If Delta | null    |
| DeltaWeightFunction   | Weight function for delta files.                                                            | No       | null    |
| DeltaGetNextFile      | Function to fetch the next delta file. Enables incremental data loading.                    | If Delta | null    |
| DeltaInterval         | Interval for polling new delta files.                                                       | No       | null    |
| FilesHaveHeader       | Whether the CSV files include a header row.                                                 | No       | false   |


## XML Files

The XML File Source Connector allows you to ingest structured data from XML files using a defined XML schema (XSD). Each XML file is parsed using the provided schema, and a specific element is used to represent rows of data.

### Adding the Connector

To add the XML file source connector to the `ConnectorManager`, use the `AddXmlFileSource` method:

```csharp
connectorManager.AddXmlFileSource("my_xml_table", new XmlFileOptions()
{
    FileStorage = Files.Of.LocalDisk("./xmldata"),
    XmlSchema = File.ReadAllText("schemas/invoice.xsd"),
    ElementName = "Invoice",
    GetInitialFiles = async (storage, state) => new[] { "invoices1.xml", "invoices2.xml" }
});
```

Once registered, the connector can be queried like any other source in Flowtide:

```sql
SELECT * FROM my_xml_table
```

### Options

| Name              | Description                                                                     | Required | Default |
| ----------------- | ------------------------------------------------------------------------------- | -------- | ------- |
| FileStorage       | Storage backend where XML files are located (e.g., local disk, Azure Blob, S3). | Yes      | -       |
| XmlSchema         | The XML Schema Definition (XSD) that describes the structure of the XML data.   | Yes      | -       |
| ElementName       | The name of the XML element to treat as a row in the result set.                | Yes      | -       |
| GetInitialFiles   | Function that returns a list of XML file paths to load on startup.              | Yes      | -       |
| BeforeBatch       | Optional hook to run before each batch is processed.                            | No       | null    |
| DeltaGetNextFiles | Function to fetch the next delta files. Enables incremental data loading.       | If Delta | null    |
| DeltaInterval     | Interval for polling new delta files.                                           | No       | null    |
| ExtraColumns      | Optional list of extra columns                                                  | No       | null    |	

## Text Lines File Source

The Text Lines File Source allows you to ingest line-based files into a Flowtide stream. Each line in the file is treated as an individual row, and metadata such as the file name is also made available as a column. This connector is useful for processing logs, newline-delimited JSON, or simple flat text files.

### Adding the connector

To register the connector in your `ConnectorManager`, use the `AddTextLinesFileSource` extension method:

```csharp
connectorManager.AddTextLinesFileSource("my_text_lines", new TextLinesFileOptions()
{
    FileStorage = Files.Of.LocalDisk("./logdata"),
    GetInitialFiles = async (fs, state) => new[] { "events.log" }
});
```

Once registered, you can query it using Flowtide:

```sql
SELECT fileName, value FROM my_text_lines
```

This will return each line in the file as a row with two columns:

* `fileName`: The name of the file that the line came from.
* `value`: The content of the line.


### Options


| Name              | Description                                                               | Required | Default |
| ----------------- | ------------------------------------------------------------------------- | -------- | ------- |
| FileStorage       | Storage backend to read files from (e.g., local disk, Azure Blob, S3).	| Yes      | -       |
| GetInitialFiles   | Function returning initial set of files to read during startup.	        | Yes      | -       |
| DeltaGetNextFiles | Optional function to return new files to ingest over time.                | No       | null    |
| DeltaInterval     | Optional interval for polling for new files.                              | No       | null    |
| BeforeBatch       | Hook to run before each batch. Can be used to prepare state.              | No       | null    |
| ExtraColumns      | Optional list of extra columns                                            | No       | null    |		
