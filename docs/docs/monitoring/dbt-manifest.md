---
sidebar_position: 5
---

# dbt Manifest

Flowtide can serve stream lineage as a mock dbt `manifest.json` and `catalog.json` over HTTP.
Data catalogs that ingest dbt artifacts, such as [DataHub](https://datahubproject.io/) and [OpenMetadata](https://open-metadata.org/), can then pull table and column lineage from Flowtide.

* Every table a stream writes becomes a dbt **model**.
* Every table that feeds a model becomes a dbt **source**.
* Column lineage is carried by mock SQL in each model's `compiled_code`. The catalogs' SQL parsers turn that SQL back into the column lineage Flowtide computed.

## Setup with Dependency Injection

Install the following NuGet packages:

* FlowtideDotNet.DependencyInjection
* FlowtideDotNet.AspNetCore

Add the following code to your *Program.cs*:

```csharp
builder.Services.AddFlowtideDbtManifest(opt =>
{
    opt.MapNamespace("mssql", database: "shop", defaultSchema: "dbo");
});

builder.Services.AddFlowtideStream("orders")
    // ...
    .AddDbtManifest();

var app = builder.Build();

app.MapFlowtideDbtManifest("/dbt");
```

* `AddDbtManifest` opts a stream in. All opted-in streams share one `DbtManifestStore`.
* `AddFlowtideDbtManifest` configures the store once for the host. It can be called any number of times.
* A `DbtManifestStore` singleton registered before these calls is used instead of the default one. Call `ExpectStream` on it yourself to get the warm-up behaviour.

## Setup with FlowtideBuilder

Create a store, pass it to each stream with `WithDbtManifestStore`, and map it:

```csharp
var store = new DbtManifestStore(new DbtManifestOptions()
    .MapNamespace("mssql", database: "shop", defaultSchema: "dbo"));

var stream = new FlowtideBuilder("orders")
    .AddPlan(plan)
    .AddConnectorManager(connectorManager)
    .WithStateOptions(stateOptions)
    .WithDbtManifestStore(store)
    .Build();

app.MapFlowtideDbtManifest(store);
```

* Lineage needs a connector manager. Without one, a warning is logged and the stream is not registered.
* The stream is registered after a successful `Build()`. A lineage failure is logged and never fails `Build()`.
* The options are copied when the store is created, so later changes to them are ignored.

### Distributed streams

Each substream registers separately, and the store merges the substreams of a stream under its logical name.

With `DistributedStreamBuilder`:

```csharp
distributedStreamBuilder.ConfigureSubstream((substreamName, builder) => builder.WithDbtManifestStore(store));
```

With Orleans, register the store as a singleton and set it in `ConfigureBuilder`:

```csharp
var store = new DbtManifestStore();
services.AddSingleton(store);

services.AddFlowtideOrleans(connectors => { ... }, (streamName, substreamName, storage) => { ... },
    options =>
    {
        options.ConfigureBuilder = (streamName, substreamName, flowtideBuilder) =>
        {
            flowtideBuilder.WithDbtManifestStore(store);
        };
    });
```

Each process serves only the substreams built in it. There is no aggregation across a cluster.

## Routes

`MapFlowtideDbtManifest` maps four GET routes under its prefix (default `/dbt`):

| Route                          | Content                               |
| ------------------------------ | ------------------------------------- |
| `{prefix}/manifest.json`          | Manifest for every registered stream. |
| `{prefix}/catalog.json`           | Catalog for every registered stream.  |
| `{prefix}/{stream}/manifest.json` | Manifest for one stream.              |
| `{prefix}/{stream}/catalog.json`  | Catalog for one stream.               |

| Situation                         | Response                                                       |
| --------------------------------- | -------------------------------------------------------------- |
| Normal                            | 200 `application/json` with `ETag` and `Cache-Control: no-cache` |
| `If-None-Match` matches the ETag  | 304                                                            |
| Unknown stream                    | 404                                                            |
| Warming up                        | 503 with `Retry-After: 10`                                     |
| Generating the artifact failed    | 500, and the error is logged                                   |

**Warm-up.** Streams opted in with `AddDbtManifest` are expected by the store.
Every route returns 503 until all expected streams have registered, or until `WarmupTimeout` (default 2 minutes) has passed since the store was created.
A catalog then fails that ingestion run instead of ingesting an empty manifest and soft-deleting existing entities.
With `FlowtideBuilder`, call `store.ExpectStream("orders")` to get the same behaviour.

**Authorization.** `MapFlowtideDbtManifest` returns the route group, so conventions apply to all four routes:

```csharp
app.MapFlowtideDbtManifest().RequireAuthorization("lineage");
```

**Ingest only the combined routes.** A table written by one stream and read by another is a model in the writer's manifest and a source in the reader's manifest.
Separate ingestions of per-stream routes overwrite each other. The per-stream routes are meant for debugging.

## Configuration Options

| Option                 | Type                                     | Default                       | Description                                                                                                   |
| ---------------------- | ---------------------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------- |
| ProjectName            | `string`                                 | `flowtide`                    | dbt package name, used as prefix in every `unique_id`.                                                        |
| SqlDialect             | `DbtSqlDialect`                          | `Postgres`                    | Dialect of the mock SQL and of `metadata.adapter_type`: `Postgres`, `TSql` or `Snowflake`.                    |
| ExcludedNamespaces     | `ISet<string>`                           | `console`, `blackhole`, `test` | Namespaces left out. Matches the full namespace or the part before `://`.                                     |
| IncludeConnectorSchema | `bool`                                   | `true`                        | Asks connectors for their table schema at build. The SQL Server connector queries metadata for every table.  |
| WarmupTimeout          | `TimeSpan`                               | 2 minutes                     | Longest time the routes answer 503 while expected streams are missing.                                        |
| RelationResolver       | `Func<DbtRelationContext, DbtRelation?>?` | `null`                        | Overrides the database, schema and identifier of a table. Returning `null` keeps the default.                 |

Configuration binding only adds to `ExcludedNamespaces`; the default exclusions can only be removed in code.

## Mapping Tables to dbt Relations

A catalog links a dbt node to an existing table by database, schema and identifier, so those must match the names the catalog already has.
Each connector reports a namespace, such as `mssql`, `elasticsearch` or `kafka://broker:9092`, and a table name. They are resolved in this order:

1. Tables in an excluded namespace are left out.
2. A `MapNamespace` mapping for the full namespace is used, otherwise one for the part before `://`.
3. Built-in defaults: `elasticsearch` and `kafka*` are flat, so the whole table name is the identifier (topic `orders.v1` stays one identifier). `mssql` gets the default schema `dbo`.
4. The table name is split on `.` (catalog prefixes are already removed):

   | Parts               | Database                          | Schema                     | Identifier |
   | ------------------- | --------------------------------- | -------------------------- | ---------- |
   | `orders`            | mapped database                   | mapped default schema, or empty | `orders`   |
   | `dbo.orders`        | mapped database                   | `dbo`                      | `orders`   |
   | `shop.dbo.orders`   | `shop`                            | `dbo`                      | `orders`   |
   | more than 3 parts   | leading parts joined with `.`     | second to last part        | last part  |

5. `RelationResolver` is called last, and a non-null result wins.

```csharp
builder.Services.AddFlowtideDbtManifest(opt =>
{
    opt.RelationResolver = ctx =>
        ctx.Namespace == "mssql" && ctx.DefaultRelation.Identifier.StartsWith("dwh_")
            ? new DbtRelation("dwh", ctx.DefaultRelation.Schema, ctx.DefaultRelation.Identifier)
            : null;
});
```

An exception from the resolver fails the request with 500 and logs the table name.

* **SQL Server databases.** The SQL Server connector reports every table under the namespace `mssql`. Two databases cannot be told apart from one or two part names without a resolver, or without three part names in the SQL.
* **Casing.** Identifiers keep the physical casing reported by the connector and are always quoted in the SQL. Column names follow the connector schema when `IncludeConnectorSchema` is on.
* **Same relation in two namespaces.** Catalogs key a node by database, schema and identifier, never by namespace. When tables in two namespaces resolve to the same relation, compared case-insensitively, the catalog sees one table. A stream that copies `mssql` table `shop.dbo.orders` to a `shop.dbo.orders` table in another store is an example: DataHub gives both nodes one dbt URN, and OpenMetadata shows the model reading from itself. Both nodes get the meta key `flowtide_relation_collision` with the other node's `unique_id`. Keep them apart by excluding one of the namespaces, or, when the catalog knows the tables under different names, by mapping one namespace to its own database with `MapNamespace` (one and two part names) or with a `RelationResolver` keyed on `ctx.Namespace`:

  ```csharp
  opt.RelationResolver = ctx => ctx.Namespace == "starrocks"
      ? new DbtRelation("analytics", ctx.DefaultRelation.Schema, ctx.DefaultRelation.Identifier)
      : null;
  ```

* **Node names.** Sources get `source.{project}.{ns}_{database}_{schema}.{identifier}` and models get `model.{project}.{ns}__{database}__{schema}__{identifier}`, where `ns` is the namespace before `://`. Every part of these ids, and of the model `name`, is lowercased, and each character outside `[a-z0-9_]` becomes `_`. Empty parts are left out, and colliding names get a hash suffix, such as `kafka_84f9e20e`. Only the source `name` and `identifier` and the model `alias` keep the physical name. For example, table `Shop.DBO.Orders` in `mssql` becomes `source.flowtide.mssql_shop_dbo.orders`, and topic `orders.v1` in `kafka://broker:9092` becomes `source.flowtide.kafka.orders_v1`.

## DataHub

Point the dbt source at the combined routes, one recipe per target platform:

```yaml
source:
  type: dbt
  config:
    manifest_path: "https://flowtide.example.com/dbt/manifest.json"
    catalog_path: "https://flowtide.example.com/dbt/catalog.json"
    target_platform: mssql
    drop_duplicate_sources: false
    node_name_pattern:
      allow:
        - '^(source|model)\.flowtide\.mssql[_.]'
```

* **One platform per recipe.** DataHub uses one `target_platform` for every node in a recipe. When streams span platforms, for example SQL Server to Elasticsearch, use one recipe per platform with a `node_name_pattern` that matches that platform's nodes, and the same `env` and `platform_instance` in each.
* **Patterns match sanitized ids.** `node_name_pattern` is matched against the `unique_id`, which is lowercased with `_` in place of other characters (see **Node names** above). To allow table `Shop.DBO.Orders` alone, match `source.flowtide.mssql_shop_dbo.orders`, not the physical name.
* **Duplicate sources.** Set `drop_duplicate_sources: false` in every recipe. With the default, DataHub drops a source that has the same name as a model and points references to it at that model, so a `flowtide_relation_collision` pair becomes a model that reads from itself. Flowtide never emits one table as both a source and a model, so the setting only matters for such pairs.
* **Authentication.** DataHub fetches the URLs with a plain GET and a 30 second timeout. The only option is basic auth embedded in the URL, such as `https://user:password@flowtide.example.com/dbt/manifest.json`.
* **URN casing.** Keep `convert_urns_to_lowercase` consistent with the recipe that ingested the warehouse tables, so the URNs match.
* **Column casing.** Set `convert_column_urns_to_lowercase: true` when DataHub parses the SQL with a case-insensitive dialect:

  | Dialect             | DataHub v1.7 and later | DataHub v1.6 and earlier                                     |
  | ------------------- | ---------------------- | ------------------------------------------------------------ |
  | `Postgres`          | Not needed             | Needed when `target_platform` is case-insensitive (mssql, snowflake) |
  | `TSql`, `Snowflake` | Needed                 | Same as `Postgres`                                           |

  DataHub v1.6 and earlier parse the SQL with `target_platform` instead of `adapter_type`, so non-SQL platforms such as elasticsearch or kafka get no column lineage there.

## OpenMetadata

Add a dbt agent to the database service (**Settings → Services → Database Services → service → Agents → Add dbt Agent**), choose the HTTP configuration and set:

* `dbtManifestHttpPath` to `https://flowtide.example.com/dbt/manifest.json`
* `dbtCatalogHttpPath` to `https://flowtide.example.com/dbt/catalog.json` (optional)

Notes:

* The tables must already exist in OpenMetadata, ingested by its own connector. The dbt agent only attaches models and lineage to them.
* Exclude namespaces that are not tables in OpenMetadata, such as kafka, elasticsearch and mongodb. Their nodes have an empty database and schema, which OpenMetadata matches by name against unrelated tables:

  ```csharp
  builder.Services.AddFlowtideDbtManifest(opt =>
  {
      opt.ExcludedNamespaces.Add("kafka");
      opt.ExcludedNamespaces.Add("elasticsearch");
      opt.ExcludedNamespaces.Add("mongodb");
  });
  ```

* Column lineage only resolves source tables in the same database service as the target table.
* OpenMetadata 1.x cannot send custom headers, so the routes must be reachable without them. OpenMetadata 2.0 adds `dbtHttpHeaders`.

## Limitations

* The SQL is lineage only. It shows which columns feed which column, not the real transformation, and is not meant to be run.
* Transformation types, such as `DIRECT/AGGREGATION`, only appear in the column meta key `flowtide_inputs`. `flowtide_streams` lists the writing streams on models and the reading streams on sources.
* Tables in different namespaces with the same database, schema and identifier look like one table to the catalogs. Such nodes carry `flowtide_relation_collision`.
* Filters pushed into a read and filters inside views are not visible in the lineage.
* In DataHub, a model that feeds a model on another platform resolves to the recipe's platform.
* Registrations last for the life of the process. A substream that is removed keeps serving its lineage until the process restarts.
