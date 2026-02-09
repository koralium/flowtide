---
sidebar_position: 6
---

# Datetime Functions

## Strftime

:::warning

This function exists for backwards compatibility and compliance with substrait. If possible please use [Timestamp Format](#timestamp-format).
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

## Timestamp Add

*This function has no substrait equivalent*

Add a specified time interval to a timestamp value. The result is a new timestamp value with the interval added.

### Supported Components

* `YEAR` Add the specified number of years.
* `QUARTER` Add the specified number of quarters (3-month intervals).
* `MONTH` Add the specified number of months.
* `WEEK` Add the specified number of weeks (7-day intervals).
* `DAY` Add the specified number of days.
* `HOUR` Add the specified number of hours.
* `MINUTE` Add the specified number of minutes.
* `SECOND` Add the specified number of seconds.
* `MILLISECOND` Add the specified number of milliseconds.
* `MICROSECOND` Add the specified number of microseconds.

### SQL Usage

```sql
SELECT TIMESTAMP_ADD(component, amount, timestamp) FROM ...
SELECT TIMESTAMP_ADD('DAY', 7, timestamp_column) FROM ...
SELECT TIMESTAMP_ADD('MONTH', 3, timestamp_column) FROM ...
```

## Datediff

*This function has no Substrait equivalent*

Calculates the number of datepart boundaries crossed between two timestamp values.  
The result is an integer representing how many complete intervals of the given component exist between the `start` and `end` timestamps.

This behavior matches **SQL Server’s `DATEDIFF`**, meaning it counts **boundary crossings**, not elapsed time.  
For example, one minute boundary is crossed between `12:00:00` and `12:01:00`, even if only one second of that minute passes.

### Supported Components

* `YEAR` — Number of year boundaries crossed.  
* `QUARTER` — Number of quarter (3-month) boundaries crossed.  
* `MONTH` — Number of month boundaries crossed.  
* `WEEK` — Number of week boundaries crossed (weeks start on Sunday).  
* `DAY` — Number of day boundaries crossed (midnight-to-midnight).  
* `HOUR` — Number of hour boundaries crossed.  
* `MINUTE` — Number of minute boundaries crossed.  
* `SECOND` — Number of second boundaries crossed.  
* `MILLISECOND` — Number of millisecond boundaries crossed.  
* `MICROSECOND` — Number of microsecond boundaries crossed.

### SQL Usage

```sql
SELECT DATEDIFF(component, start, end) FROM ...;

-- Examples
SELECT DATEDIFF('DAY', '2024-01-01T00:00:00', '2024-01-02T00:00:00');   
SELECT DATEDIFF('MINUTE', '2025-01-01T12:00:00', '2025-01-01T12:01:00'); 
SELECT DATEDIFF('MONTH', order_date, ship_date);
```


## Round Calendar

[Substrait definition](https://substrait.io/extensions/functions_datetime/#round_calendar)

Round a temporal value to a calendar boundary using a specified rounding strategy, unit, and origin. This follows the Substrait model and allows flexible alignment to calendar-based periods.

**Signature**

round_calendar(x, rounding, unit, origin, multiple)

**Parameters**

- **x**  
  Timestamp value to be rounded.

- **rounding**  
  Rounding strategy to apply:
  - **FLOOR** Round down to the nearest boundary.
  - **CEIL** Round up to the nearest boundary.
  - **ROUND_TIE_DOWN** Round to the nearest boundary; ties round downward.
  - **ROUND_TIE_UP** Round to the nearest boundary; ties round upward.

- **unit**  
  Target unit to round to:
  - **YEAR** Round to a year boundary.
  - **MONTH** Round to a month boundary.
  - **WEEK** Round to a week boundary (as defined by `origin`).
  - **DAY** Round to a day boundary.
  - **HOUR** Round to an hour boundary.
  - **MINUTE** Round to a minute boundary.
  - **SECOND** Round to a second boundary.
  - **MILLISECOND** Round to a millisecond boundary.

- **origin**  
  Calendar alignment reference used when rounding:
  - **YEAR** Align to the start of the calendar year.
  - **MONTH** Align to the start of each month.
  - **MONDAY_WEEK** Weeks begin on Monday.
  - **SUNDAY_WEEK** Weeks begin on Sunday.
  - **ISO_WEEK** ISO 8601 week definition; weeks start Monday and the first week has the majority of its days in January.
  - **US_WEEK** US epidemiological week definition; weeks start Sunday and the first week has the majority of its days in January.
  - **DAY** Align to day boundaries.
  - **HOUR** Align to hour boundaries.
  - **MINUTE** Align to minute boundaries.
  - **SECOND** Align to second boundaries.
  - **MILLISECOND** Align to millisecond boundaries.

- **multiple**  
  Integer factor specifying grouping of the chosen unit (for example, every 5 minutes, every 2 months, etc.).

### SQL Usage

```sql
SELECT round_calendar(val, 'FLOOR', 'MONTH', 'YEAR', 1) FROM ...
SELECT round_calendar(val, 'CEIL', 'WEEK', 'MONDAY_WEEK', 1) FROM ...
SELECT round_calendar(val, 'ROUND_TIE_UP', 'MINUTE', 'HOUR', 5) FROM ...