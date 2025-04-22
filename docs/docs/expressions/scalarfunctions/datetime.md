---
sidebar_position: 6
---

# Datetime Functions

## Strftime

:::warning

This function exists for backwards compatability and compliance with substrait. If possible please use [Timestamp Format](#timestamp-format).

:::

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

## Floor timestamp day

*This function has no substrait equivalent*

Rounds a timestamp down to the nearest day by removing the time portion (hours, minutes, seconds, etc.). If the input is null or not of the type timestamp, the result is null.

### SQL Usage

```sql
SELECT floor_timestamp_day(timestamp_column) FROM ...
```

## Timestamp Parse

The `timestamp_parse` function converts a string representation of a date and/or time into a `timestamp` value, using a specified format string.

The format string follows the .NET DateTime format, allowing for flexible and expressive parsing patterns. This includes support for components like year (yyyy), month (MM), day (dd), hour (HH/hh), minute (mm), second (ss), fractional seconds (fff), time zone offsets (zzz), and more.

The format string follows the [.NET DateTime format](https://learn.microsoft.com/en-us/dotnet/standard/base-types/custom-date-and-time-format-strings).

If the input string does not match the provided format exactly, the function will return `NULL`.

```sql
-- Basic date parsing
SELECT TIMESTAMP_PARSE('2025-04-16', 'yyyy-MM-dd');

-- Date and time parsing with 24-hour format
SELECT TIMESTAMP_PARSE('2025-04-16 14:30:45', 'yyyy-MM-dd HH:mm:ss');
```

## Timestamp Extract

[Substrait definition](https://substrait.io/extensions/functions_datetime/#extract)

Extract portion of a timestamp value. This follows the substrait description and has the following component values:

* YEAR Return the year.
* ISO_YEAR Return the ISO 8601 week-numbering year. First week of an ISO year has the majority (4 or more) of
  its days in January.
* US_YEAR Return the US epidemiological year. First week of US epidemiological year has the majority (4 or more)
  of its days in January. Last week of US epidemiological year has the year's last Wednesday in it. US
  epidemiological week starts on Sunday.
* QUARTER Return the number of the quarter within the year. January 1 through March 31 map to the first quarter,
  April 1 through June 30 map to the second quarter, etc.
* MONTH Return the number of the month within the year.
* DAY Return the number of the day within the month.
* DAY_OF_YEAR Return the number of the day within the year. January 1 maps to the first day, February 1 maps to
  the thirty-second day, etc.
* MONDAY_DAY_OF_WEEK Return the number of the day within the week, from Monday (first day) to Sunday (seventh
  day).
* SUNDAY_DAY_OF_WEEK Return the number of the day within the week, from Sunday (first day) to Saturday (seventh
  day).
* MONDAY_WEEK Return the number of the week within the year. First week starts on first Monday of January.
* SUNDAY_WEEK Return the number of the week within the year. First week starts on first Sunday of January.
* ISO_WEEK Return the number of the ISO week within the ISO year. First ISO week has the majority (4 or more)
  of its days in January. ISO week starts on Monday.
* US_WEEK Return the number of the US week within the US year. First US week has the majority (4 or more) of
  its days in January. US week starts on Sunday.
* HOUR Return the hour (0-23).
* MINUTE Return the minute (0-59).
* SECOND Return the second (0-59).
* MILLISECOND Return number of milliseconds since the last full second.
* MICROSECOND Return number of microseconds since the last full millisecond.

### SQL Usage

```sql
SELECT timestamp_extract(component, val) FROM ...
SELECT timestamp_extract('YEAR', val) FROM ...
SELECT timestamp_extract('QUARTER', val) FROM ...
```

## Timestamp Format

The `timestamp_format` function formats a `timestamp` value into a string representation using a specified format string.

The format string follows the .NET DateTime format. This includes support for components like year (`yyyy`), month (`MM`), day (`dd`), hour (`HH`/`hh`), minute (`mm`), second (`ss`), fractional seconds (`fff`), time zone offsets (`zzz`), and more.

The format string follows the [.NET DateTime format](https://learn.microsoft.com/en-us/dotnet/standard/base-types/custom-date-and-time-format-strings).

If the input timestamp is `NULL` or the format string is `NULL`, the function will return `NULL`.

### SQL Usage

```sql
SELECT TIMESTAMP_FORMAT(timestampColumn, 'yyyy-MM-dd');
```