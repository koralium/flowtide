---
sidebar_position: 8
---

# List Functions

## List Sort Ascending Null Last

*This function has no substrait equivalent*

Sorts a list of values in ascending order, placing any null values at the end of the result.

Values are ordered according to their natural ascending order (e.g., numerically or lexicographically).
Nulls are not compared to other values directly; they are always considered greater for the purpose of ordering.
Any value that is not a list will return the result as null.

### SQL Usage

```sql
SELECT list_sort_asc_null_last(list(orderkey, userkey)) FROM ...
```

## List First Difference

*This function has no substrait equivalent*

Returns the first element from the first list where a difference is found when compared to the corresponding element in the second list. The comparison is done element-by-element, maintaining the order of the lists. If no difference is found (i.e., all elements in both lists are equal, or the first list is shorter than the second list), the function returns NULL.

If one or both of the lists are NULL, the function handles them as follows:
* If the second list is NULL, the function returns the first element of the first list (if it exists).
* If the first list is NULL, the function returns NULL.

### SQL Usage

```sql
SELECT list_first_difference(list1, list2) FROM ...
```

## List Filter Null

*This function has no substrait equivalent*

Removes all NULL elements from a list, returning a new list that retains the original order of the non-NULL elements.

If the input list is NULL, the function returns NULL. If the list contains only NULL values, the result is an empty list.

This function is useful for cleaning data where NULL values may represent missing, irrelevant, or default entries that should be excluded from further processing.

### SQL Usage

```sql
SELECT list_filter_null(list_column) FROM ...
```