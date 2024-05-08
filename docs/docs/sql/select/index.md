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

All fields which says expression, can take in expressions found under the [Expressions chapter](/docs/category/expressions).

## Wildcards

Wildcards can be used in SQL projections, as demonstrated in the example below:

```sql
SELECT * FROM table1
```

However, using wildcards is generally discouraged. They can lead to the inclusion of unnecessary columns and may result in errors if the structure of the source table changes.