---
sidebar_position: 2
---

# Datetime Functions

## Hopping Window

*This function has no substrait equivalent*

* **Extension URI:** /functions_datetime.yaml
* **Extension Name:** hopping_window

Hopping window, also called sliding window, returns one row for each window a timestamp belongs to.
It adds two columns:

| Column         | Type      | Description                    |
|----------------|-----------|--------------------------------|
| `window_start` | timestamp | Start of the window, inclusive |
| `window_end`   | timestamp | End of the window, exclusive   |

Each window is *size* long and a new window starts every *hop*.
The window starts are aligned from year one, which means that a one week window starts on a monday.
A timestamp belongs to a window if it is on or after *window_start* and before *window_end*.

Arguments:

1. Timestamp to assign to windows
2. Hop amount, the distance between two window starts
3. Hop unit
4. Size amount, the length of a window
5. Size unit

The amounts must be integer literals, and the units one of *WEEK*, *DAY*, *HOUR*, *MINUTE*, *SECOND*, *MILLISECOND* or *MICROSECOND*.
Fractions are not allowed, use a smaller unit instead, for example *342 SECOND* instead of *5.7 MINUTE*.
Calendar units such as *MONTH* and *YEAR* are not supported since their length varies.

A timestamp falls into *size / hop* windows, rounded up:

* If the hop is smaller than the size, the windows overlap and a timestamp belongs to several windows.
* If the hop is equal to the size, the windows tumble and a timestamp belongs to exactly one window.
* If the hop is larger than the size, there are gaps between the windows and a timestamp can belong to no window.

Since each row is duplicated once per window, a small hop together with a large size is rejected when the stream starts.
The limit is 100 000 windows per row.

If the input is null or not a timestamp, no windows are returned.

### SQL Usage

#### Assigning rows to windows

```sql
-- An order at 00:07 with a 5 minute hop and 10 minute size
-- belongs to the windows starting at 00:00 and 00:05
SELECT window_start, window_end
FROM orders o
INNER JOIN hopping_window(o.order_time, 5, 'MINUTE', 10, 'MINUTE')
```

#### Aggregating per window

The most common usage is to assign the rows to windows and then aggregate per window:

```sql
SELECT
  window_start,
  count(*) as order_count
FROM orders o
INNER JOIN hopping_window(o.order_time, 5, 'MINUTE', 10, 'MINUTE')
GROUP BY window_start
```

#### Left join

```sql
SELECT o.id, window_start
FROM orders o
LEFT JOIN hopping_window(o.order_time, 10, 'MINUTE', 5, 'MINUTE')
```

When used in a LEFT JOIN, rows are still returned even if the timestamp does not belong to any window.
The window columns are then null.
