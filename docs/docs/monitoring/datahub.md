---
sidebar_position: 6
---

# DataHub

Flowtide can serve stream lineage over HTTP the way a [DataHub](https://datahubproject.io/) server does, so DataHub can pull it with its built-in `datahub` source, the source that replicates one DataHub instance into another.
The lineage arrives as native DataHub metadata, so no SQL is parsed and every dataset keeps its own platform.

* Every stream becomes a **data flow** with the orchestrator `flowtide`.
* Every table a stream writes becomes a **data job** in that flow, with the tables it reads as inputs and column lineage between them.
* Every table read or written becomes a **dataset** with a schema, unless turned off.

## Setup with Dependency Injection

Install the following NuGet packages:

* FlowtideDotNet.DependencyInjection
* FlowtideDotNet.AspNetCore

Add the following code to your *Program.cs*:

```csharp
builder.Services.AddFlowtideDataHubLineage(opt =>
{
    opt.MapNamespace("mssql", m => m.Database = "shop");
});

builder.Services.AddFlowtideStream("orders")
    // ...
    .AddDataHubLineage();

var app = builder.Build();

app.MapFlowtideDataHubLineage("/datahub");
```

* `AddDataHubLineage` opts a stream in. All opted-in streams share one `DataHubLineageStore`.
* `AddFlowtideDataHubLineage` configures the store once for the host. It can be called any number of times.
* A `DataHubLineageStore` singleton registered before these calls is used instead of the default one. Call `ExpectStream` on it yourself to get the warm-up behaviour.

## Setup with FlowtideBuilder

Create a store, pass it to each stream with `WithDataHubLineageStore`, and map it:

```csharp
var store = new DataHubLineageStore(new DataHubLineageOptions()
    .MapNamespace("mssql", m => m.Database = "shop"));

var stream = new FlowtideBuilder("orders")
    .AddPlan(plan)
    .AddConnectorManager(connectorManager)
    .WithStateOptions(stateOptions)
    .WithDataHubLineageStore(store)
    .Build();

app.MapFlowtideDataHubLineage(store, "/datahub");
```

* Lineage needs a connector manager. Without one, a warning is logged and the stream is not registered.
* The stream is registered after a successful `Build()`. A lineage failure is logged and never fails `Build()`.
* The options are copied when the store is created, so later changes to them are ignored.

Distributed streams register each substream separately, and the store merges the substreams of a stream into one data flow.
Set the store with `ConfigureSubstream` on a `DistributedStreamBuilder`, or in `ConfigureBuilder` with Orleans, as described for the [dbt manifest](dbt-manifest.md#distributed-streams).
Each process serves only the substreams built in it.

## DataHub Recipe

Point `datahub_api` at the route prefix and turn on `pull_from_datahub_api`:

```yaml
pipeline_name: "flowtide-lineage"
datahub_api:
  server: "https://flowtide.example.com/datahub"
  token: "${FLOWTIDE_TOKEN}"
source:
  type: datahub
  config:
    pull_from_datahub_api: true
```

* **UI ingestion.** Create a source with a custom recipe and leave the sink out. The data then goes to the DataHub instance that runs the ingestion, and the schedule, run history and secrets are managed by DataHub. DataHub sets `pipeline_name` itself.
* **CLI ingestion.** Add a `datahub-rest` sink that points at your DataHub GMS, and keep `pipeline_name`. The source has stateful ingestion on by default, which fails to start without it.
* **Always set a non-empty token.** The value can be anything Flowtide accepts. Without a token, or with one that resolves to an empty string, the DataHub CLI sends the `DATAHUB_SYSTEM_CLIENT_ID` and `DATAHUB_SYSTEM_CLIENT_SECRET` environment variables to the `datahub_api` server as basic authentication, and the DataHub ingestion executor has both set.
* **Stateful ingestion** can stay at its default. Flowtide reports that no checkpoint exists, and the source stores none.
* **Expected warning.** With CLI ingestion and a `datahub-rest` sink, the CLI tries to write a run report to the `datahub_api` server at the start of a run. Flowtide refuses the write, so the log shows `Reporting failed on start`. The run summary still reaches DataHub through the sink.
* **CLI version.** The `pull_from_datahub_api` option is hidden from the DataHub documentation. Tested with DataHub 1.7 (CLI 1.7.0.2). Pin the CLI version of the ingestion source so a DataHub upgrade does not change the client unnoticed.

The source reads every entity on every run, one request per entity, and writes each aspect in full.

## Routes

`MapFlowtideDataHubLineage` maps the calls the `datahub` source makes, under its prefix (default `/datahub`):

| Route                          | Content                                                       |
| ------------------------------ | ------------------------------------------------------------- |
| `GET {prefix}/config`             | Server configuration the CLI checks before a run.            |
| `POST {prefix}/api/graphql`       | `scrollAcrossEntities`, pages through every entity urn.      |
| `GET {prefix}/entitiesV2/{urn}`   | Aspects of one entity. An unknown urn has no aspects.         |
| `POST {prefix}/aspects`           | `getTimeseriesAspectValues` returns nothing, writes are refused. |

**Warm-up.** Streams opted in with `AddDataHubLineage` are expected by the store.
The routes return 503 with `Retry-After: 10` until all expected streams have registered, or until `WarmupTimeout` (default 2 minutes) has passed since the store was created, so an early run fails instead of replicating part of the lineage.
With `FlowtideBuilder`, call `store.ExpectStream("orders")` to get the same behaviour.

**Authorization.** The routes are anonymous unless a convention is applied, and they expose every dataset name, schema, column lineage and custom aspect.
`MapFlowtideDataHubLineage` returns the route group, so conventions apply to all routes. The CLI sends the recipe token as `Authorization: Bearer <token>`:

```csharp
app.MapFlowtideDataHubLineage().RequireAuthorization("lineage");
```

## Configuration Options

| Option                 | Type                                                    | Default                        | Description                                                                                  |
| ---------------------- | ------------------------------------------------------- | ------------------------------ | -------------------------------------------------------------------------------------------- |
| Env                    | `string`                                                | `PROD`                         | Environment of datasets, flows and jobs. Must be a DataHub environment such as `PROD`, `DEV` or `QA`. `CERT` needs DataHub 1.7 or later. |
| ExcludedNamespaces     | `ISet<string>`                                          | `console`, `blackhole`, `test` | Namespaces left out. Matches the full namespace or the part before `://`.                    |
| IncludeConnectorSchema | `bool`                                                  | `true`                         | Asks connectors for their table schema at build and uses it for dataset schemas. The SQL Server connector queries metadata for every table. |
| IncludeDatasetMetadata | `bool`                                                  | `true`                         | Serves status and schema for every dataset. See **Dataset Metadata**.                        |
| WarmupTimeout          | `TimeSpan`                                              | 2 minutes                      | Longest time the routes answer 503 while expected streams are missing.                       |
| DatasetResolver        | `Func<DataHubDatasetContext, DataHubDataset?>?`         | `null`                         | Overrides the platform, name, platform instance and environment of a table. Returning `null` keeps the default. |
| AspectProvider         | `Func<DataHubEntityContext, IEnumerable<DataHubAspect>?>?` | `null`                      | Adds aspects to flows, jobs and datasets. See **Custom Aspects**.                            |

Configuration binding only adds to `ExcludedNamespaces`; the default exclusions can only be removed in code. `MapNamespace`, `DatasetResolver` and `AspectProvider` can only be set in code.

`DatasetResolver` and `AspectProvider` run when the entities are generated, on the first request after a stream registers. One generation runs at a time, on a request thread. An exception from either fails the routes with 500 and is logged, until the next registration.

`MapNamespace(namespace, configure)` sets the following for one namespace. The namespace is matched in full first, then by the part before `://`, ignoring case:

| Option                 | Description                                                                 |
| ---------------------- | --------------------------------------------------------------------------- |
| Platform               | DataHub platform id. Defaults to the namespace, with `postgresql` as `postgres` and `delta_table` as `delta-lake`. |
| PlatformInstance       | Platform instance, prefixed to the dataset name like DataHub's own sources do. |
| Env                    | Environment of the namespace's datasets.                                    |
| Database               | Database for one and two part table names.                                  |
| DefaultSchema          | Schema for one part table names. `mssql` defaults to `dbo`.                 |
| LowercaseNames         | Lowercases dataset names, platform instance prefix included, for recipes with `convert_urns_to_lowercase`. |
| LowercaseColumns       | Lowercases column names, for recipes with `convert_column_urns_to_lowercase`. |
| IncludeDatasetMetadata | Overrides `IncludeDatasetMetadata` for the namespace.                       |

## Mapping Tables to DataHub Datasets

Lineage only connects to the tables DataHub already has when the dataset urns match the ones DataHub's own sources create.
The name of a dataset is built from the connector's table name:

* `elasticsearch` and `kafka` keep the table name as is, so topic `orders.v1` is dataset `orders.v1`.
* Other namespaces split the name on `.` and join database, schema and table, as described for the [dbt manifest](dbt-manifest.md#mapping-tables-to-dbt-relations). `mssql` table `orders` with `Database = "shop"` becomes `shop.dbo.orders`.
* The platform instance, when set, is prefixed: `prod-sql.shop.dbo.orders`.
* `DatasetResolver` is called last, and a non-null result wins. Its result is used as is, so `LowercaseNames` does not apply to it, while the namespace's `IncludeDatasetMetadata` and `LowercaseColumns` still do.

```csharp
builder.Services.AddFlowtideDataHubLineage(opt =>
{
    opt.MapNamespace("mssql", m =>
    {
        m.Database = "shop";
        m.PlatformInstance = "prod-sql";
        m.LowercaseNames = true;
        m.LowercaseColumns = true;
    });
    opt.DatasetResolver = ctx => ctx.TableName.StartsWith("archive.")
        ? new DataHubDataset("mssql", "archive.dbo." + ctx.NameParts[^1].ToLowerInvariant(), "archive-sql")
        : null;
});
```

* **SQL Server databases.** The SQL Server connector reports every table under the namespace `mssql`. Use three part names in the SQL, `Database`, or a resolver to tell databases apart.
* **Casing.** Column names follow the connector schema when `IncludeConnectorSchema` is on. DataHub draws a column edge only when the field exists in the dataset schema with the same casing. Columns are matched ignoring case, so two columns of one table that differ only in casing become one field.
* **Length.** DataHub drops urns longer than 512 characters once URL encoded, without an error. A data job whose urn would be longer gets a hashed id, while a dataset or flow urn that long is dropped by DataHub.

## Dataset Metadata

With `IncludeDatasetMetadata` on, every dataset in the lineage gets a `status` aspect, a `schemaMetadata` aspect built from the connector or plan schema, and a `dataPlatformInstance` aspect when a platform instance is set.
This makes datasets that no other DataHub source ingests, such as an Elasticsearch index, show up with their columns, and DataHub only draws column lineage between datasets that have a schema.

Turn it off for namespaces that another DataHub source ingests, so the two sources do not replace each other's schema on every run:

```csharp
opt.MapNamespace("mssql", m => m.IncludeDatasetMetadata = false);
```

The data jobs still link to those datasets, and `AspectProvider` can still add aspects to them.

## Custom Aspects

`AspectProvider` is called for every flow, job and dataset in the lineage and can add any DataHub aspect.
The value is the aspect in the JSON form the GMS API returns, and an aspect with the same name as a built-in one replaces it:

```csharp
opt.AspectProvider = ctx => ctx.EntityType == DataHubEntityType.DataFlow
    ? [
        new DataHubAspect("ownership", JsonNode.Parse("""
            {"owners":[{"owner":"urn:li:corpGroup:data-platform","type":"TECHNICAL_OWNER"}],"ownerTypes":{},"lastModified":{"time":0,"actor":"urn:li:corpuser:flowtide"}}
            """)!),
        new DataHubAspect("globalTags", JsonNode.Parse("""{"tags":[{"tag":"urn:li:tag:streaming"}]}""")!)
      ]
    : null;
```

An aspect name the CLI does not know is dropped with a warning in the ingestion log, and an invalid value fails the run.

## Limitations

* Nothing is ever removed. A stream or table that disappears stays in DataHub until it is deleted there, for example with `datahub delete`.
* Every aspect is written in full on every run, so a dataset aspect such as `schemaMetadata` also written by another source flips between the two versions.
* Filters pushed into a read and filters inside views are not visible in the lineage.
* Each process serves only the substreams built in it. When substreams of one stream in different processes write the same table, their data jobs have the same urn and replace each other.
* Registrations last for the life of the process.
* A data job's id is built from its output dataset alone, as `platform.name`. Outputs in another environment than `Env`, platforms whose id contains a dot, and names containing `~` get `~env~` and a hash of the dataset urn appended. Renaming or remapping an output creates a new job and leaves the old one in DataHub.
