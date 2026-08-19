---
sidebar_position: 3
---

# Datetime Functions

## Session Window

*This function has no substrait equivalent*

* **Extension URI:** /functions_datetime.yaml
* **Extension Name:** session_window

Session window assigns each row the start of the session it belongs to.
A session is a run of rows where no two neighbouring timestamps are further apart than the *gap*.
The first row of a run starts a new session, and every following row within the gap of its predecessor belongs to the same session.

Unlike a hopping or a tumbling window, a session is not decided by the timestamp alone, it depends on the other rows in the partition.
Adding a row between two sessions merges them, and removing a row can split a session in two.

The rows are ordered by the *order by* column, so it is that column the gap is measured on.

Arguments:

1. Gap amount, the largest distance between two neighbouring rows that still belong to the same session
2. Gap unit

The amount must be an integer literal, and the unit one of *WEEK*, *DAY*, *HOUR*, *MINUTE*, *SECOND*, *MILLISECOND* or *MICROSECOND*.
Fractions are not allowed, use a smaller unit instead, for example *342 SECOND* instead of *5.7 MINUTE*.
Calendar units such as *MONTH* and *YEAR* are not supported since their length varies.

A distance equal to the gap keeps the rows in the same session, a distance greater than the gap starts a new one.

The function requires exactly one *order by* column, and it must be ascending and a timestamp.
A second order by column would make the neighbouring row something other than the next timestamp.

*Partition by* is optional. Without it every row is part of the same run.

If the order by value is null or not a timestamp, no session is returned.
Such rows sort together at one end and do not split the run around them.

### SQL Usage

#### Assigning rows to sessions

```sql
-- A user is in the same session as long as there is less than
-- 30 minutes between two of their events
SELECT userid, eventtime,
       session_window(30, 'MINUTE') OVER (PARTITION BY userid ORDER BY eventtime) AS session_start
FROM events
```

#### Sessions over all rows

```sql
-- Without a partition by, one run over the whole stream
SELECT eventtime,
       session_window(30, 'MINUTE') OVER (ORDER BY eventtime) AS session_start
FROM events
```

> [!NOTE]
> When a row is added or removed, every following row up to the next gap can get a new session start.
> A change at the beginning of a long session therefore re-emits the entire session.

## Session Windows in GROUP BY

A session can also be used directly as a grouping, which computes the session window before the aggregate and groups on its result.
The partition is taken from the other grouping columns, so it does not have to be repeated inside the *SESSION* call.

The grouping is available as a column named *window_start*, the same name the hopping and tumbling table functions produce.

Arguments:

1. Timestamp to assign to sessions
2. Gap amount
3. Gap unit

Only one *SESSION* expression is allowed in a *GROUP BY*.

### SQL Usage

#### Aggregating per session

```sql
SELECT
  userid,
  count(*) AS event_count,
  window_start
FROM events
GROUP BY userid, SESSION(eventtime, 30, 'MINUTE')
```

#### Getting the end of the session

*SESSION_END* returns the point where the session is over, which is the last timestamp in the session plus the gap.
It is an aggregate, so it can be used in any query that groups on a session.

```sql
SELECT
  userid,
  count(*) AS event_count,
  window_start,
  SESSION_END(eventtime, 30, 'MINUTE') AS window_end
FROM events
GROUP BY userid, SESSION(eventtime, 30, 'MINUTE')
```

The start can also be written as *SESSION_START*, which returns the same column as *window_start*.

```sql
SELECT
  userid,
  count(*) AS event_count,
  SESSION_START(eventtime, 30, 'MINUTE') AS window_start,
  SESSION_END(eventtime, 30, 'MINUTE') AS window_end
FROM events
GROUP BY userid, SESSION(eventtime, 30, 'MINUTE')
```

Both accessors take the same arguments as the *SESSION* in the group by, and they must match it.
A query that asks for the start or the end of a different session than the one it groups on is rejected when the stream starts.
