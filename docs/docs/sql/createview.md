---
sidebar_position: 3
---

# Create View

The *create view* command allows you to define a reusable sub-plan.
When the SQL is converted into substrait, the view plan is added as a relation, and any usage of the view results in a [Reference Relation](https://substrait.io/relations/logical_relations/#reference-operator).

If the same data should be accessable in multiple parts in a stream, creating a view is helpful to reduce having to redo the computation.

Example:

```sql
CREATE VIEW testview AS
SELECT c1 + 1 as column FROM testtable;

INSERT INTO outputtable
SELECT column FROM testview;
```

## Buffered view

It is possible to create a buffered view.
It is a view that collects all the output from the view in to temporary storage and waits for a watermark.

This can reduce the output from the view in situations where data is updated regularly but gives the same output value.
One example is when a column value is based on *gettimestamp* such as *gettimestamp() > date* which will give the same output
the majority of the time. The buffered view will then only give out the changed rows, and can reduce computational load in the result of the stream.

It adds a [buffer operator](/docs/operators/buffer) at the end of the view.

Example:

```sql
CREATE VIEW buffered WITH (BUFFERED = true) AS
SELECT
    CASE WHEN orderdate < gettimestamp() THEN true ELSE false END as active
FROM orders;
```