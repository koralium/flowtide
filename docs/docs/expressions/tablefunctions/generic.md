---
sidebar_position: 1
---

# Generic Functions

## Unnest

*There is not substrait definition for unnest*

* **Extension URI:** /functions_table_generic.yaml
* **Extension Name:** unnest

Unnest is a table function which takes in one argument that can be a list of element or a map.
If it is a list then it returns one row for each entry in the list.
If it is a map, it will return an object with that looks as the following:

```
{ key: "fieldName", value: fieldValue }
```  

If it is a list, it will return the element as it was in the list.

An example with a list would be the list [1, "test", 3] returns three rows with values:

| Value   |
|---------|
| 1       |
| "test"  |
| 3       |

In the case of an object *\{ field1: "test", field2: "test2" \}* it would return:

| Value                                     |
|-------------------------------------------|
| \{ key: "field1", value: "test" \}        |
| \{ key: "field2", value: "test2" \}       |


### SQL Usage

#### In a *FROM* statement

```sql
--- Returns three rows with 1, 2, 3 as val
SELECT val FROM UNNEST(list(1,2,3)) val
```

#### With a left join

```sql
SELECT 
  id, 
  element_value 
FROM documents d
LEFT JOIN UNNEST(d.list) element_value
```

When used in a LEFT JOIN, rows are still returned even if the list is empty.

#### Left join with a condition

```sql
SELECT 
  id, 
  element_value 
FROM documents d
LEFT JOIN UNNEST(d.list) element_value ON element_value = 123
```

In the above example all rows are returned but *element_value* is only set if it is equal to *123*.

#### Inner join

```sql
SELECT 
  id, 
  element_value 
FROM documents d
INNER JOIN UNNEST(d.list) element_value
```

When used in an INNER JOIN, only rows that have elements in the list will be returned.
Inner join works the same with conditions as left joins, but rows are not returned with a null value if not matched to the condition.
