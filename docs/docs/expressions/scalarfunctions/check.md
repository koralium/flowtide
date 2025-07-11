---
sidebar_position: 7
---

# Check Functions

Check functions help validate data quality and raise alerts for specific data issues.
To visualize check results, you need to add a `CheckFailureListener` to the stream.

```csharp
builder.Services.AddFlowtideStream("stream")
  .WriteCheckFailuresToLogger() // Outputs check results to the logger — a good starting point
```

## Check value

*This function has no substrait equivalent*

`check_value` takes in at least three arguments:

* **Scalar value** - this value is passed through and returned by the function.
* **Condition** - boolean condition of the check
* **Message** - String message that should be logged if the check fails.

After these three arguments, any extra arguments are added as tags.

### SQL Usage

```SQL
select 
  CHECK_VALUE(column1, column1 < column2, 'Column1 is larger than column2: {column1} > {column2}', column1, column2) as val
FROM ...
```

## Check true

*This function has no substrait equivalent*

`check_true` takes in at least two arguments:

* **Condition** - boolean condition of the check, the result of this check is returned.
* **Message** - String message that should be logged if the check fails.

After these two arguments, any extra arguments are added as tags.

### SQL Usage

```SQL
select 
  *
FROM ...
WHERE CHECK_TRUE(column1 < column2, 'Column1 is larger than column2: {column1} > {column2}', column1, column2)
```