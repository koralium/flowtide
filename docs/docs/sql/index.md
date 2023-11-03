---
sidebar_position: 6
---

import DocCardList from '@theme/DocCardList';



# SQL

Flowtide has SQL support which will transform the SQL into a substrait plan which can then be run the engine.

To create a plan from SQL, add the following code to your application:

```csharp
var sqlBuilder = new SqlPlanBuilder();

sqlBuilder.Sql(@"
CREATE TABLE testtable (
  val any
);

INSERT INTO output
SELECT t.val FROM testtable t
");

var plan = sqlBuilder.GetPlan();
```

:::info

All SQL plans must have a source and a sink, so it must always insert to somewhere.
The INSERT INTO denotes which output should leave the stream.

:::

You can find more information in the following chapters:

<DocCardList />