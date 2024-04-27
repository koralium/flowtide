---
sidebar_position: 3
---

import DocCardList from '@theme/DocCardList';

# Table Functions

Table functions are specialized functions that returns multiple rows.
An example of a table function is 'UNNEST' which takes in a list of data and returns one row for each element in the list.

In Flowtide, table functions are used with the *TableFunctionRelation*. 

There is no definition yet in substrait for table functions and its relations, so a custom definition is created for flowtide.
It uses the *ExtensionSingleRel* if used with an input. The input is only required when it is used in joins.
If no input is used, *ExtensionLeafRel* can be used instead.
If the table function is a root, for instance in SQL: 

```sql
... FROM func(arg)
```
No input is not required.

The following proto definition is used to define the *TableFunctionRelation*:

```protobuf
message TableFunction {
    // Points to a function_anchor defined in this plan, which must refer
    // to a table function in the associated YAML file. Required; 0 is
    // considered to be a valid anchor/reference.
    uint32 function_reference = 1;

    // Arguments for the table function
    repeated substrait.FunctionArgument arguments = 7;
}

message TableFunctionRelation {
    // Table function to use
    TableFunction table_function = 1;

    // Schema of the output table from the function
    substrait.NamedStruct table_schema = 2;

    // Only required if used with an input.
    // Only left and inner join supported at this time.
    JoinType type = 6;

    // Optional, but only usable with an input.
    substrait.Expression join_condition = 4;

    enum JoinType {
      JOIN_TYPE_UNSPECIFIED = 0;
      JOIN_TYPE_INNER = 1;
      JOIN_TYPE_LEFT = 3;
    }
}
```

See below for the different table functions:

<DocCardList />