---
sidebar_position: 4
---

# Select

The select statement allows a user to write the actual query that will fetch data from different connectors and transform it into a destination.

Below is how the select statement is written in ANTLR format:

```sql
SELECT scalar_or_aggregate_expression (',' scalar_or_aggregate_expression)*
FROM table_source
((LEFT | INNER)? JOIN table_source ON scalar_expression)*
(WHERE scalar_expression)?
(GROUP BY scalar_expression)?
(HAVING scalar_or_aggregate_expression)?
```

All fields which says expression, can take in expressions found under the [Expressions chapter](../../expressions/scalarfunctions/arithmetic.md).

## Wildcards

Wildcards can be used in SQL projections, as demonstrated in the example below:

```sql
SELECT * FROM table1
```

However, using wildcards is generally discouraged. They can lead to the inclusion of unnecessary columns and may result in errors if the structure of the source table changes.

## Subqueries

Flowtide supports logical subquery decorrelation for `EXISTS`, `NOT EXISTS`, and `IN` predicates inside `WHERE` clauses.

### EXISTS and NOT EXISTS
You can filter rows based on the presence or absence of matching rows in a subquery:

```sql
SELECT u.userkey
FROM users u
WHERE EXISTS (
  SELECT 1 FROM orders o WHERE o.userkey = u.userkey
)
```

And for `NOT EXISTS`:

```sql
SELECT u.userkey
FROM users u
WHERE NOT EXISTS (
  SELECT 1 FROM orders o WHERE o.userkey = u.userkey
)
```

### IN and NOT IN Subqueries
You can also filter rows using `IN` or `NOT IN` with a subquery:

```sql
SELECT u.userkey
FROM users u
WHERE u.userkey IN (
  SELECT o.userkey FROM orders o
)
```

And for `NOT IN`:

```sql
SELECT u.userkey
FROM users u
WHERE u.userkey NOT IN (
  SELECT o.userkey FROM orders o
)
```

### Implementation Details
Flowtide compiles these subqueries into a logical `LeftMark` join relation. During plan optimization, correlated filters inside the subquery are pulled up and mapped into the join conditions. This decorrelation avoids row duplication and optimizes stream performance by suppressing redundant updates.
