---
sidebar_position: 6
---

# Datetime Functions

## Strftime

[Substrait definition](https://substrait.io/extensions/functions_datetime/#strftime)

Converts a timestamp in microseconds since 1970-01-01 00:00:00.000000 to a string.
See [strftime](https://man7.org/linux/man-pages/man3/strftime.3.html) on how to to write the format string.

Arguments:

1. Timestamp in microseconds
2. Format as a string

### SQL Usage

```sql
INSERT INTO output
SELECT
    strftime(Orderdate, '%Y-%m-%d %H:%M:%S') as Orderdate
FROM Orders
```

## Get timestamp

*This function has no substrait equivalent*

:::warning

Get timestamp is not yet supported inside of join conditions.

:::

:::info

If you do comparisons with *gettimestamp* it may be benificial to use buffered views to only send out changed rows to the rest of the stream.
Please see [Buffered view](/docs/sql/createview#buffered-view).

:::

Get the current timestamp (current datetime).

This is a meta function that itself does no computation, but is instead replaced during the optimization step with
joins against a timestamp data source.

Example:

```kroki type=blockdiag
  blockdiag {
    Source -> Project -> Write
  }
```

Where if project uses gettimestamp becomes:

```kroki type=blockdiag
  blockdiag {
    Source -> CrossJoin -> Project -> Write;
    TimestampProvider -> CrossJoin;
  }
```

This means that all rows that are evaluated will have the same value of *gettimestamp* to ensure consistent results.

The timestamp provider also publishes a watermark called '__timestamp' which can be used to see the current time that has been evaluted in the stream.

By default the timestamp is updated every hour. This is configurable during the stream setup:

```csharp
streamBuilder
    .SetGetTimestampUpdateInterval(TimeSpan.FromHours(12))
```

### SQL Usage

```sql
INSERT INTO output
SELECT
    gettimestamp() as CurrentDateTime
FROM Orders
```

