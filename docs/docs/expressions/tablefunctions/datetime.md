---
sidebar_position: 2
---

# Datetime Functions

## Hopping Window

*There is no substrait definition for hopping_window*

* **Extension URI:** /functions_datetime.yaml
* **Extension Name:** hopping_window

Hopping window (also known as a sliding window) assigns a timestamp to every hopping window it
falls into. For each input row it returns one row per window, adding two columns:

| Column         | Type      | Description                          |
|----------------|-----------|--------------------------------------|
| `window_start` | timestamp | Inclusive start of the window        |
| `window_end`   | timestamp | Exclusive end of the window          |

A window covers the interval `[window_start, window_end)` where `window_end = window_start + size`,
and window starts are aligned to multiples of the hop from the epoch. A timestamp belongs to a
window when `window_start <= timestamp < window_end`.

The signature is:

```sql
hopping_window(timestamp, hop_amount, hop_unit, size_amount, size_unit)
```

* `timestamp` – the timestamp expression to assign to windows. A non-timestamp or null value
  produces no windows (under a LEFT join a single row with null window columns is returned).
* `hop_amount`, `hop_unit` – the distance between consecutive window starts (the slide).
* `size_amount`, `size_unit` – the length of each window.

`hop_amount`/`size_amount` must be integer literals and `hop_unit`/`size_unit` must be one of the
fixed-duration units `WEEK`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND` or `MICROSECOND`.
Calendar units such as `MONTH` and `YEAR` are not supported because their length varies.

Each timestamp falls into `ceil(size / hop)` windows:

* When `hop < size` the windows overlap, so a timestamp belongs to several windows.
* When `hop = size` the windows tumble (no overlap), so a timestamp belongs to exactly one window.
* When `hop > size` there are gaps between windows, so a timestamp in a gap belongs to no window.

Because every row fans out into `ceil(size / hop)` rows, a very small hop combined with a large
size is rejected at query start (the limit is 100,000 windows per row) to avoid exhausting memory.

### SQL Usage

#### Assigning rows to windows

```sql
--- An order at 00:07 with a 5 minute hop and 10 minute size falls into
--- [00:00, 00:10) and [00:05, 00:15)
SELECT window_start, window_end
FROM orders o
INNER JOIN hopping_window(o.order_time, 5, 'MINUTE', 10, 'MINUTE')
```

#### Aggregating per window

The typical use is to fan rows into windows and then aggregate per window:

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

When used in a LEFT JOIN, a row is still returned with null window columns when the timestamp
falls into no window (for example when it lands in a gap between non-overlapping windows).
