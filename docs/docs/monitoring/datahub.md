---
sidebar_position: 6
---

# DataHub

Flowtide can serve stream lineage over HTTP the way a [DataHub](https://datahubproject.io/) server does, so DataHub can pull it with its built-in `datahub` source, the source that replicates one DataHub instance into another.
The lineage arrives as native DataHub metadata, so no SQL is parsed and every dataset keeps its own platform.

* Every stream becomes a **data flow** with the orchestrator `flowtide`.
* Every table a stream writes becomes a **data job** in that flow, with the tables it reads as inputs and column lineage between them.
* Every table read or written becomes a **dataset** with a schema, unless turned off.
* The `flowtide` **data platform** gets a display name and a logo, unless turned off.
* Every [check](../expressions/scalarfunctions/check.md) becomes an **assertion** on the datasets it guards, with its latest status, unless turned off.
* Every data job gets a **run** per stream, or per substream of a distributed stream, with its current state, unless turned off.

Install the NuGet package `FlowtideDotNet.Lineage.DataHub`. Everything below is in the namespace `FlowtideDotNet.Lineage.DataHub`.

## Setup with Dependency Injection

Add the following code to your *Program.cs*:

```csharp
builder.Services.AddFlowtideStream("orders")
    // ...
    .AddDataHubLineage(opt =>
    {
        opt.MapNamespace("mssql", m => m.Database = "shop");
    });

var app = builder.Build();

app.MapFlowtideDataHubLineage("/datahub");
```

* `AddDataHubLineage` opts a stream in. All opted-in streams share one `DataHubLineageStore` and one set of options, so the options it configures apply to every stream. When several streams configure them, all callbacks run in registration order and a later one wins on a conflict.
* `AddFlowtideDataHubLineage` configures the same options without a stream, for example in a shared startup method. It can be called any number of times.
* A `DataHubLineageStore` singleton registered before these calls is used instead of the default one, and the options callbacks then have no effect. Call `ExpectStream` on it yourself to get the warm-up behaviour.

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

### Distributed streams

Each substream registers separately, and the store merges the substreams of a stream into one data flow under its logical name.

With `DistributedStreamBuilder`:

```csharp
distributedStreamBuilder.ConfigureSubstream((substreamName, builder) => builder.WithDataHubLineageStore(store));
```

With Orleans, register the store as a singleton and set it in `ConfigureBuilder`:

```csharp
var store = new DataHubLineageStore();
services.AddSingleton(store);

services.AddFlowtideOrleans(connectors => { ... }, (streamName, substreamName, storage) => { ... },
    options =>
    {
        options.ConfigureBuilder = (streamName, substreamName, flowtideBuilder) =>
        {
            flowtideBuilder.WithDataHubLineageStore(store);
        };
    });
```

Each process serves only the substreams built in it. There is no aggregation across a cluster.

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
| ExcludedNamespaces     | `ISet<string>`                                          | empty                          | Namespaces left out, such as `console` or `blackhole`. Matches the full namespace or the part before `://`. |
| IncludeConnectorSchema | `bool`                                                  | `true`                         | Asks connectors for their table schema at build and uses it for dataset schemas. The SQL Server connector queries metadata for every table. |
| IncludeDatasetMetadata | `bool`                                                  | `true`                         | Serves status and schema for every dataset. See **Dataset Metadata**.                        |
| WarmupTimeout          | `TimeSpan`                                              | 2 minutes                      | Longest time the routes answer 503 while expected streams are missing.                       |
| DatasetResolver        | `Func<DataHubDatasetContext, DataHubDataset?>?`         | `null`                         | Overrides the platform, name, platform instance and environment of a table. Returning `null` keeps the default. |
| AspectProvider         | `Func<DataHubEntityContext, IEnumerable<DataHubAspect>?>?` | `null`                      | Adds aspects to flows, jobs, datasets and assertions. See **Custom Aspects**.                |
| IncludePlatformInfo    | `bool`                                                  | `true`                         | Serves the `flowtide` data platform. See **Flowtide Platform**.                              |
| PlatformLogoUrl        | `string?`                                               | The Flowtide logo on GitHub    | Logo of the `flowtide` data platform. `null` leaves the logo out.                            |
| IncludeChecks          | `bool`                                                  | `true`                         | Serves checks as assertions with their latest status. See **Data Quality Checks**.           |
| RaiseIncidents         | `bool`                                                  | `false`                        | Raises an incident while a check fails and resolves it when the check passes. Needs `IncludeChecks`. See **Incidents**. |
| IncidentPriority       | `DataHubIncidentPriority`                               | `Medium`                       | Priority of raised incidents, `Critical`, `High`, `Medium` or `Low`, and the severity of failing checks. |
| IncidentPriorityResolver | `Func<DataHubIncidentContext, DataHubIncidentPriority?>?` | `null`                     | Overrides the priority and severity per check. Returning `null` keeps `IncidentPriority`.    |
| IncludeRuns            | `bool`                                                  | `true`                         | Serves a run on each data job per stream, or per substream, with its current state. See **Runs**. |

Configuration binding adds to `ExcludedNamespaces`. `MapNamespace`, `DatasetResolver`, `AspectProvider` and `IncidentPriorityResolver` can only be set in code.

`DatasetResolver`, `AspectProvider` and `IncidentPriorityResolver` run when the entities are generated, on the first request after a stream registers. One generation runs at a time, on a request thread. An exception from any of them, or a priority outside `DataHubIncidentPriority`, fails the routes with 500 and is logged, until the next registration.

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
* Other namespaces split the name on `.`, with catalog prefixes already removed, and join database, schema and table:

  | Parts             | Database                      | Schema                          | Table     |
  | ----------------- | ----------------------------- | ------------------------------- | --------- |
  | `orders`          | `Database`                    | `DefaultSchema`, or empty       | `orders`  |
  | `dbo.orders`      | `Database`                    | `dbo`                           | `orders`  |
  | `shop.dbo.orders` | `shop`                        | `dbo`                           | `orders`  |
  | more than 3 parts | leading parts joined with `.` | second to last part             | last part |

  Empty parts are left out, and `mssql` defaults `DefaultSchema` to `dbo`, so `mssql` table `orders` with `Database = "shop"` becomes `shop.dbo.orders`.
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

## Column Lineage

Every column a data job writes is linked to the columns it is computed from, with the kind of transformation, such as `DIRECT:IDENTITY` or `DIRECT:AGGREGATION`.

Join keys, filter columns and group by keys decide which rows are written, so they are linked to every column the job writes, as `INDIRECT:JOIN`, `INDIRECT:FILTER` or `INDIRECT:GROUP_BY`.
Impact analysis on such a column, for example a join key that is never selected, then reaches the table the job writes.

* A column used in several ways is listed once, with the use closest to the written table. A group by key that is also a join key shows as `INDIRECT:GROUP_BY`.
* DataHub's OpenLineage integration ignores these columns, so a stream sent to DataHub through the [OpenLineage reporter](openlineage.md) does not get them.

## Dataset Metadata

With `IncludeDatasetMetadata` on, every dataset in the lineage gets a `status` aspect, a `schemaMetadata` aspect built from the connector or plan schema, and a `dataPlatformInstance` aspect when a platform instance is set.
This makes datasets that no other DataHub source ingests, such as an Elasticsearch index, show up with their columns, and DataHub only draws column lineage between datasets that have a schema.

Turn it off for namespaces that another DataHub source ingests, so the two sources do not replace each other's schema on every run:

```csharp
opt.MapNamespace("mssql", m => m.IncludeDatasetMetadata = false);
```

The data jobs still link to those datasets, and `AspectProvider` can still add aspects to them.

## Data Quality Checks

Checks made with `CHECK_VALUE` and `CHECK_TRUE` are served as DataHub assertions, listed in the **Quality** tab of a dataset.
A check becomes one assertion for every table the rows it checks are written to. A check whose rows reach no served table has no assertion.

Each assertion is a custom assertion of type `Flowtide Check` with the check message as its description.
Its run event carries the status that Flowtide last committed:

| `CheckState`   | DataHub result | `unexpectedCount` | `nativeResults`                |
| -------------- | -------------- | ----------------- | ------------------------------ |
| `NotEvaluated` | `SUCCESS`      | 0                 | `activeIssues`, `failingRows`  |
| `Passed`       | `SUCCESS`      | 0                 | `activeIssues`, `failingRows`  |
| `Failed`       | `FAILURE`      | Failing rows      | `activeIssues`, `failingRows`  |

A failing run event also carries a severity, which DataHub shows on the failure. It comes from `IncidentPriority` or `IncidentPriorityResolver`, whether or not incidents are raised. DataHub has three severities, so `Critical` and `High` both become high.

* **Checks need a running stream.** The status comes from the stream that runs the check, so an assertion has no run event until the stream has started.
* **Only the latest status.** Each run of the ingestion source writes the status at that moment. A check that fails and passes again between two runs shows only the pass. The run event keeps its time until the status changes, so runs without a change add no new event. After a stream or substream is rebuilt, its assertions keep their last result until the rebuilt checks report.
* **Not evaluated is a success.** A new check is `NotEvaluated` until its first checkpoint. It is served as `SUCCESS` rather than DataHub's `INIT`, because DataHub counts `INIT` as failing in the dataset health, which would mark the dataset as failing during startup.
* **Identity.** The assertion urn is a hash of the stream name, the dataset and the check message. Edits that keep the message keep the assertion. Changing the message, or writing to another table, creates a new assertion, and the old one stays in DataHub with its last status. Give every check on a table its own message, because checks with the same message on a table are told apart only by their order in the plan.
* **Distributed streams.** The copies of a check in each substream report as one assertion. It fails when any copy fails, and adds up the failing rows of copies that check different partitions. Copies in other processes are not seen, as described under **Limitations**.

`flowtide.checkIds` in the assertion's custom properties lists the ids that check status listeners receive for the check.
Set `IncludeChecks = false` to serve no assertions.

## Incidents

Open source DataHub does not raise incidents from assertion results by itself, an assertion's `assertionActions` have no effect there.
With `RaiseIncidents` on, Flowtide raises the incident itself: while a check fails, its dataset has an active incident, and when the check passes again the incident is resolved.

```csharp
builder.Services.AddFlowtideDataHubLineage(opt =>
{
    opt.RaiseIncidents = true;
    opt.IncidentPriority = DataHubIncidentPriority.High;
    opt.IncidentPriorityResolver = ctx => ctx.CheckMessage.StartsWith("Order")
        ? DataHubIncidentPriority.Critical
        : null;
});
```

* **One incident per assertion.** It is titled with the check message, has the type `Flowtide Check`, and links to its assertion. A failure after a pass opens the same incident again with a new start time.
* **Datasets with failing checks.** An active incident makes the dataset match the **Has Active Incidents** filter, so a view can list the datasets with failing checks. The **Has Failing Assertions** filter does not work in open source DataHub for any assertion.
* **Only checks that failed.** A check that never failed in this process has no incident.
* **Removed checks.** When a stream is rebuilt in the same process without a check that has an incident, the incident is resolved. After a restart or redeploy without the check, Flowtide no longer knows the incident, so it stays active in DataHub; resolve or delete it there.
* **Flowtide owns the incident.** Every run of the ingestion source writes the incident again, so a change made in DataHub to its status, stage, priority, assignees, title or description is undone on the next run while Flowtide still serves it. Deleting it in DataHub does not last either. Notes added in DataHub are kept.
* **Restarts.** Incidents are kept in memory. If a check passes and Flowtide restarts before the next ingestion run, its incident stays active in DataHub. Resolve it there; that lasts until the check fails again. A restart while a check fails raises its incident again with new times, which undoes changes made in DataHub once.
* **Only the latest status.** A check that fails and passes again between two runs raises no incident. Every read of the endpoint counts, so another client reading it while the check fails makes the next run write a resolved incident.

To leave Flowtide's incidents out on the DataHub side, add `urn:li:incident:.*` to the source's `urn_pattern.deny` list. The list replaces the source's default deny patterns, which match nothing Flowtide serves.

## Runs

Runs hang under the data jobs, so the shape in DataHub is data flow, data job, run. Each data job, that is each table a stream writes, has one run for the stream. In a distributed stream it has one run per substream that writes the table, named `stream/substream`. A stream or substream that writes no table itself, for example a substream that only feeds other substreams, gets its run on the data flow instead, with no output, so its failures still show in the flow's **Runs** tab.

A run lists the tables that feed its job's table and the table itself. It shows in the job's **Runs** tab, in the **Runs** tab of the table it writes, and as **Running** or **Last run** on the job in search and in the lineage graph of a flow or job. All runs of one stream or substream follow its state:

| Stream state                                        | Run status                  |
| --------------------------------------------------- | --------------------------- |
| Starting, Running                                   | Running                     |
| Failure                                             | Failed, with its duration   |
| Restarted after a failure                           | Running again               |
| Stopped, also while starting                        | Succeeded                   |
| Stopped, but the stop reported an error             | Failed                      |
| Stopped or deleted after a failure                  | Stays Failed                |
| Deleted while starting or running                   | Cancelled                   |

* **One run, not one per start.** The run's urn is fixed per job and substream, so a restarted or redeployed process takes the same run over, and the Runs tab shows the current state rather than a history of starts. The run's time is its first start since the stream was last built, so a rebuild, such as a new Orleans activation, moves it.
* **The last state stays.** A stop is only seen if an ingestion run happens before the process exits, and a crashed process reports nothing. The run then shows its last pulled state, usually Running, until a process serves it again.
* **Not started yet.** A built stream that has not started has no run.
* **Several processes.** When a substream moves to another process, the run with the newest state wins in DataHub, so the clocks of the processes should agree. The run's time can switch between the processes' start times.
* Runs are not drawn as nodes of their own in the lineage graph.
* **DataHub's cleanup.** DataHub's optional cleanup of process instances, off by default, removes job runs older than its retention, including the run of a stream that has been running longer than that.
* **OpenLineage.** Flowtide's OpenLineage reporter creates runs of its own, a new one per build. If it also sends to the same DataHub, DataHub shows both kinds of runs for the same stream, so turn one of them off.

Set `IncludeRuns = false` to serve no runs.

## Flowtide Platform

Flows and jobs belong to the data platform `flowtide`, which DataHub does not know.
Flowtide serves `urn:li:dataPlatform:flowtide` with the same `dataPlatformInfo` that `datahub put platform` writes: display name `Flowtide`, type `OTHERS` and `PlatformLogoUrl` as the logo.
The default logo is `https://raw.githubusercontent.com/koralium/flowtide/main/logo/flowtidelogo.svg`, so the browser that shows DataHub must reach GitHub. Point `PlatformLogoUrl` at a copy of the logo inside your network otherwise.

Every run writes the platform again, so a platform set up with `datahub put platform --name flowtide` is overwritten. Set `IncludePlatformInfo = false` to keep your own.
Only the `flowtide` platform is served. Dataset platforms such as `mssql` or `kafka` keep the logos and names DataHub ships with.

## Custom Aspects

`AspectProvider` is called for every flow, job, dataset and assertion in the lineage, not for incidents, and can add any DataHub aspect.
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
* Each process serves only the substreams built in it. When substreams of one stream in different processes write the same table, their data jobs have the same urn and replace each other. The same goes for the assertions and incidents of a check whose copies run in different processes, and an incident then switches between active and resolved.
* Registrations last for the life of the process.
* A data job's id is built from its output dataset alone, as `platform.name`. Outputs in another environment than `Env`, platforms whose id contains a dot, and names containing `~` get `~env~` and a hash of the dataset urn appended. Renaming or remapping an output creates a new job and leaves the old one in DataHub.
