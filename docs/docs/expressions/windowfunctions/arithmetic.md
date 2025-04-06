---
sidebar_position: 1
---

# Arithmetic Functions

## Sum

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#sum)

Calculates the sum of numeric values within a window.  
If there are no rows in the window, the result is `NULL`.  
Non-numeric values (such as strings or `NULL`s) are ignored during calculation.

### SQL Usage

```sql
 -- Total sum of all rows
SELECT SUM(column1) OVER () FROM ...
 -- Total sum of each partition
SELECT SUM(column1) OVER (PARTITION BY column2) FROM ...
-- Sum from start to current row
SELECT SUM(column1) OVER (PARTITION BY column2 ORDER BY column3) FROM ...
-- Sum of the previous four rows and current row
SELECT SUM(column1) OVER (PARTITION BY column2 ORDER BY column3 ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) FROM ...
```

## Row number

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#row_number)

The `ROW_NUMBER` window function assigns a unique, sequential number to each row within a partition of the result set. The numbering starts at 1 and is determined by the `ORDER BY` clause.

This function **requires an `ORDER BY` clause** to determine row position and **does not support frame boundaries**.    

### SQL Usage

```sql
SELECT ROW_NUMBER(column1) OVER (PARTITION BY column2 ORDER BY column3) FROM ...
```

## Lead

[Substrait definition](https://substrait.io/extensions/functions_arithmetic/#lead)

The `LEAD` window function provides access to a subsequent row’s value within the same result set partition. It returns the value of a specified column at a given offset after the current row.

If no row exists at that offset, a default value (if provided) is returned; otherwise, the result is `NULL`.

This function **requires an `ORDER BY` clause** to establish row sequence and **does not support frame boundaries**.

### SQL Usage

```sql
-- Lead with default offset 1 and null default
SELECT LEAD(column1) OVER (PARTITION BY column2 ORDER BY column3) FROM ...
-- Lead with offset 2 and null default
SELECT LEAD(column1, 2) OVER (PARTITION BY column2 ORDER BY column3) FROM ...
-- Lead with offset 2 and default value set to 'hello'
SELECT LEAD(column1, 2, 'hello') OVER (PARTITION BY column2 ORDER BY column3) FROM ...
```