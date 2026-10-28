# Flowtide expressions and functions

This catalog describes SQL names implemented by the current repository. SQL names can differ from Substrait extension names. A Substrait specification defining a function does not imply Flowtide implements it. Functions available as aggregates are not automatically available with OVER, and table functions return rows rather than scalar values.

`x`, `y`, and other identifiers below denote expressions. `...` denotes repeated arguments; `[argument]` denotes an optional argument in a signature, not literal SQL. Use the [SQL guide](https://koralium.github.io/flowtide/llms/sql.txt) for complete statements and the [stream guide](https://koralium.github.io/flowtide/llms/streams.txt) for C# integration.

## Values, nulls, and types

Runtime values include null, booleans, integers, doubles, decimals, strings, binary data, timestamps, lists, maps, and fixed-schema structs. The planner can also describe date and nested types. Metadata type support and CAST support are separate. Use explicit casts when a function expects a particular runtime type; string inputs are not automatically timestamps.

Ordinary comparisons with null can evaluate to null. WHERE keeps only true. AND/OR/NOT use three-valued logic; `IS DISTINCT FROM` and `IS NOT DISTINCT FROM` provide null-safe comparison. Numeric functions generally return null for incompatible inputs, while string and boolean helpers have their own behavior. Do not assume a single null/coercion rule applies to all functions.

## Operators and specialized expressions

| SQL form | Purpose |
| --- | --- |
| x + y, x - y, x * y, x / y, x % y, -x | Numeric arithmetic and negation. Integer division returns a floating result. Decimal inputs use decimal arithmetic where supported. |
| x = y, x <> y, x != y, x < y, x <= y, x > y, x >= y | Comparisons. |
| x AND y, x OR y, x XOR y, NOT x | Boolean logic. |
| x IS NULL, x IS NOT NULL | Null tests. |
| x IS DISTINCT FROM y, x IS NOT DISTINCT FROM y | Null-safe inequality/equality. |
| x BETWEEN lower AND upper | Inclusive range test. |
| x IN (a, b, ...), x NOT IN (a, b, ...) | Membership using SQL null logic. |
| EXISTS (query), NOT EXISTS (query), x IN (query), x NOT IN (query) | Supported filter-subquery predicates; see SQL guide. |
| x LIKE pattern [ESCAPE character], x NOT LIKE pattern | SQL Server-style patterns: % is any sequence, _ is one character, [ab] is a character set. |
| x \|\| y | String concatenation. Null propagates. |
| CASE WHEN condition THEN value ... [ELSE value] END | Conditional expression; omitted ELSE yields null. Use the searched CASE form. |
| CAST(x AS type) | Convert to STRING, BOOLEAN, INT/INTEGER/SMALLINT/TINYINT, DOUBLE/FLOAT, DECIMAL, or TIMESTAMP. Failed value conversions return null. |
| object.field | Access a map or struct field, including nested fields. |
| list(x, y, ...) | Construct a list. |
| map(key, value, ...) | Construct a map from key/value pairs; keys are converted to strings. |
| named_struct('field', value, ...) | Construct a fixed-schema struct; field names must be string literals. |
| guid(string) | Parse/normalize a GUID; this is not random GUID generation. |

Arithmetic can produce NaN or infinity for division by zero; decimal behavior must be checked separately. Use is_nan/is_finite/is_infinite when those values matter. Do not treat timestamp integer encodings as interchangeable: legacy strftime takes Unix microseconds, while integer-to-TIMESTAMP casting has its own tick conversion. Prefer typed TIMESTAMP values and timestamp_parse/timestamp_format.

See [specialized expressions](https://koralium.github.io/flowtide/docs/expressions/specializedexpressions.md) and [cast tests](https://github.com/koralium/flowtide/blob/main/tests/FlowtideDotNet.AcceptanceTests/CastTests.cs).

## Choosing a function category

Scalar functions return one value per input row. Aggregate functions maintain one result per group. Window functions use OVER (...) and preserve input rows. Table functions produce rows and belong in FROM or a supported JOIN.

Use the generated reference below for exact function signatures and restrictions. Substrait extension names can differ from SQL names; use the SQL spelling in each heading. Details about SQL statements and stream setup remain in the SQL and stream guides.
