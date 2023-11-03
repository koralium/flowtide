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
