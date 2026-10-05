---
sidebar_position: 7
---

# Check Functions

Check functions validate data quality inside a stream. Every check has a *name*, which is its message written as a
string literal, for example `'Userkey {userkey} is too large'`. Each failing row raises an *issue*, identified by the
check and the values of its tags, and the issue is *resolved* again when no row produces it anymore, for example after
the row is deleted or updated so it passes. On top of the issues, every check reports a *status*: whether it passes,
and how many issues and failing rows it has.

The plan optimizer moves every check into a [check operator](../../operators/check.md), which keeps track of the
active issues and the counts in the stream state.

There are two kinds of listeners:

* A **check failure listener** (`ICheckFailureListener`) receives every issue with its tags when it is raised or
  resolved. Use it to act on individual issues.
* A **check status listener** (`ICheckStatusListener`) receives one status per check with the number of active issues
  and failing rows, without any tags. Use it for dashboards and alerts that should not grow with the number of issues.

The built-in logger is both:

```csharp
builder.Services.AddFlowtideStream("stream")
  .WriteCheckFailuresToLogger() // Logs the status of every check, and raised and resolved issues
```

A warning is logged when the stream is built if the plan contains check functions but neither kind of listener is
registered.

## Check value

*This function has no substrait equivalent*

`check_value` takes in at least three arguments:

* **Scalar value** - this value is passed through and returned by the function.
* **Condition** - the condition of the check, an issue is raised when it is `false`.
* **Message** - a string literal, the name of the check. It can reference tags as `{tag}`.

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

* **Condition** - the condition of the check, an issue is raised when it is `false`.
* **Message** - a string literal, the name of the check. It can reference tags as `{tag}`.

After these two arguments, any extra arguments are added as tags.

The function returns `true` only when the condition is boolean `true`. It returns `false` when the condition is
`false`, `null` or not a boolean, so a row is filtered out in all those cases when it is used in a `WHERE` clause.

### SQL Usage

```SQL
select 
  *
FROM ...
WHERE CHECK_TRUE(column1 < column2, 'Column1 is larger than column2: {column1} > {column2}', column1, column2)
```

## The message is the check name

The message must be a string literal in single quotes. It is the name of the check, the same for every row, so row
values go in tags and the message references them as `{tag}`:

```SQL
-- Rejected, the message is computed per row
CHECK_VALUE(userkey, userkey < 900, concat('Userkey ', userkey, ' is too large'))

-- Supported, the row value is a tag
CHECK_VALUE(userkey, userkey < 900, 'Userkey {userkey} is too large', userkey)
```

A message that is not a string literal, such as a `concat` or `||` expression, a column or `NULL`, is rejected with a
`NotSupportedException` when the plan is optimized or the stream is built. A double quoted `"text"` is a column
reference in Flowtide SQL, not a string.

The listeners receive the message as written, with the placeholders not filled in, as `CheckName`, together with the
tags. The built-in listeners fill in each `{key}` with the value of the tag with that key, matching the key without
regard to case and writing `null` for a null value.

## Tags

Tags are added after the fixed arguments, either named or unnamed:

```SQL
CHECK_VALUE(userkey, userkey < 900, 'Userkey {key} is too large', key => userkey)
CHECK_VALUE(userkey, userkey < 900, 'Userkey {userkey} is too large', userkey)
```

A named tag uses the given name as its key. An unnamed tag uses the column name as its key, a computed expression
gets a generated name, so name the tags of computed values.

## When an issue is raised

* An issue is raised only when the condition evaluates to boolean `false`. A `null` or non-boolean condition passes.
* The tags are only evaluated for failing rows.

## Issue identity

An issue is identified by its check and its tag values. Rows with the same tag values share one issue. The issue
stays active as long as at least one row produces it, and it is resolved when the last such row is deleted or changed
to pass the check, or to produce other tag values.

A check without tags has a single issue, which is active while any row fails the check. Include the values that tell
rows apart, such as a key column, in the tags if every failing row should be its own issue.

## Check status

Every check reports its status with two counts:

* **Active issues** - the number of distinct issues that are active, that is distinct tag values with at least one
  failing row.
* **Failing rows** - the number of rows that fail the check.

A check passes when it has no active issues. For example, with the tag `company => CompanyId`, three failing rows of
two companies give two active issues and three failing rows. A check without tags has at most one active issue, and
its failing rows count every failing row.

The status is reported:

* At every start of the stream, including restarts after a stop and recoveries after a failure, for every check, from
  the restored, already committed state, before new data is processed. A passing check reports that it passes.
* After each committed checkpoint, for every check whose counts changed since its last reported status. A checkpoint
  that leaves the counts of a check unchanged reports nothing for it, also when one issue was resolved and another
  raised in it.

Like the issues, the status only covers committed state, so a checkpoint that fails is never reported.

## When listeners are called

Issues and statuses are tracked as part of the stream state and are published to the listeners only after the
checkpoint that contains them has been committed. An issue that is raised and resolved again within one checkpoint is
never published, and a checkpoint that fails is never published.

Every start of the stream publishes a snapshot of each check from the restored, already committed state, before new
data is processed: `OnCheckReset` followed by one `OnCheckFailure` per active issue, and then `OnCheckStatus`. The
snapshot replaces everything the failure listener knows about that check, so issues from a checkpoint that was rolled
back are corrected here. It is also sent when the check has no active issues, so the listener learns that the check
is empty. After the snapshot, only changes are published: `OnCheckFailure` when an issue becomes active,
`OnCheckResolved` when it is no longer active, and `OnCheckStatus` when the counts changed.

For each check the calls of one checkpoint or start come in this order: the reset of a snapshot, the issue changes,
then the status. A listener registered as both kinds therefore sees a status whose active issues match the issues it
has received for that check.

When only status listeners are registered, the check operators do not collect the issue changes. When only failure
listeners are registered, no status is collected. The counts and the metrics are always maintained.

## Listener interfaces

A custom failure listener implements `ICheckFailureListener` and is added with `WithCheckFailureListener` on the
`FlowtideBuilder`:

```csharp
public interface ICheckFailureListener
{
    // An issue became active
    void OnCheckFailure(ref readonly CheckFailureNotification notification);

    // An active issue is no longer active
    void OnCheckResolved(ref readonly CheckFailureNotification notification);

    // Forget every issue of the check, its active issues follow as OnCheckFailure calls
    void OnCheckReset(ref readonly CheckResetNotification notification);
}
```

`CheckFailureNotification` contains `StreamName`, `CheckId`, `CheckName` and `Tags`, `CheckResetNotification`
contains `StreamName`, `CheckId` and `CheckName`. `CheckName` is the message as written in the query.

A custom status listener implements `ICheckStatusListener` and is added with `WithCheckStatusListener` on the
`FlowtideBuilder`:

```csharp
public interface ICheckStatusListener
{
    // The committed status of one check
    void OnCheckStatus(ref readonly CheckStatusNotification notification);
}
```

`CheckStatusNotification` contains `StreamName`, `CheckId`, `CheckName`, `ActiveIssues`, `FailingRows` and `Passed`,
which is `true` when `ActiveIssues` is zero.

With dependency injection, a custom listener is added through `AddCustomOptions`:

```csharp
builder.Services.AddFlowtideStream("stream")
  .AddCustomOptions((provider, builder) => builder.WithCheckStatusListener(new MyStatusListener()))
```

The notifications and the tag span are only valid during the call, copy what you want to keep.

`StreamName` together with `CheckId` identifies one running check. The check id has the format
`{operatorId}:{checkIndex}`. When the stream runs in [distributed mode](../../distributed/index.md) the id is
prefixed with the substream name, `{substreamName}/{operatorId}:{checkIndex}`, and the built-in distributed hosts
give each substream its own stream name. A check that is placed in a partitioned part of the plan runs once per
partition, so one check in SQL can report under several check ids, each with the issues and counts of its own
partition. When every check has its own message, the failing rows of all check ids with the same `CheckName` add up
to the failing rows of the check. The active issues do not add up: rows with the same tag values can fail in several
partitions, so one issue can be active under several check ids, and a check without tags reports one active issue in
every partition that has a failing row. The check passes when all its check ids report that they pass.

Changes are published when a checkpoint completes, and the snapshot is published by each check operator while the
stream starts, before new data is processed. The calls of one stream are serialized but can come from different
threads, and a listener instance shared between streams or substreams is called concurrently, so a listener must be
thread safe. A slow listener delays checkpoints and stream starts, keep listeners fast. Exceptions thrown by a
listener are caught and ignored.

The built-in listeners:

| Builder method | DI method | Behavior |
| -------------- | --------- | -------- |
| `WithCheckLogger(logLevel)` | `WriteCheckFailuresToLogger(logLevel)` | A failure and a status listener. Logs `Check failed: ...` and `Check resolved: ...` at the given level, `Warning` by default, with the tags filled into the check name, and a reset at debug level. The check id and the tags are added as structured properties. Logs the status at information level as `Check passed: {CheckName}` or `Check failed: {CheckName}, {ActiveIssues} issues, {FailingRows} failing rows`. |
| `WithCheckActivityLogger()` | `WriteCheckFailuresAsActivity()` | A failure listener. Starts a `CheckFailure` or `CheckResolved` activity on the `FlowtideDotNet.CheckFailures` activity source, with the check id, the check name, the message with the tags filled in and the tags as activity tags. Resets are ignored. |

The logger gives every kind of entry its own event id and message template, so structured log sinks can tell a
raised issue from a resolved one:

| Event id | Event name | Level | Message template | Properties |
| -------- | ---------- | ----- | ---------------- | ---------- |
| 1 | `CheckFailed` | The given level | `Check failed: ` followed by the check name | The tags and `CheckId` |
| 2 | `CheckResolved` | The given level | `Check resolved: ` followed by the check name | The tags and `CheckId` |
| 3 | `CheckStatus` | Information | `Check passed: {CheckName}` or `Check failed: {CheckName}, {ActiveIssues} issues, {FailingRows} failing rows` | `CheckName`, `ActiveIssues`, `FailingRows` and `CheckId` |
| 4 | `CheckReset` | Debug | `Check reset: {CheckName}, {CheckId}` | `CheckName` and `CheckId` |

The `CheckId` property of an issue is left out when one of its tags has the key `CheckId`. Sinks that render the
message template themselves, such as Serilog, fill in the `{tag}` placeholders from the tag properties, matching the
key by exact case.

The [check operator](../../operators/check.md#metrics) also exposes the counts of every check as the metrics
`flowtide_check_active_issues` and `flowtide_check_failing_rows`, labeled with the check name.

## Where checks are evaluated

A check is evaluated once for every row that reaches the relation it is written in, with these exceptions where
only part of an expression is evaluated:

* **CASE** - a check inside a `THEN` or `ELSE` branch is only evaluated for rows that take that branch, and a check
  in a `WHEN` condition only for rows where no earlier condition matched.
* **COALESCE** - a check in an argument is only evaluated for rows where all earlier arguments are `null`.
* **CONCAT and GREATEST** - these stop at the first `null` argument, so a check in a later argument is only
  evaluated for rows where the earlier arguments are not `null`. For `CONCAT` this applies from the second argument
  and for `GREATEST` from the third, and not when `CONCAT` ignores nulls.
* **AND, OR and IN** evaluate all their arguments, so a check inside them is evaluated for every row. The top
  level `AND` of a `WHERE` clause is the exception, see below.
* **WHERE** - the conditions combined with `AND` that do not contain a check are applied first, so the check only
  sees the rows that pass them. In `WHERE CHECK_TRUE(c, '...') AND a > 1`, the check is only evaluated for rows
  where `a > 1`.
* **Aggregate measures** - a check in a measure argument is only evaluated for rows that pass the measure's
  `FILTER (WHERE ...)` clause.
* **Join conditions** - a check in an `ON` clause that only uses columns from one side of the join is evaluated
  on all rows of that side, also the rows that have no match on the other side. This includes the conditions of any
  `CASE` the check is written in.
* **Window functions** - a check in a window function argument is evaluated once for every input row, also for
  the value argument of `LEAD` and `LAG`.

## Checks that use the current time

A check can use [gettimestamp](datetime.md#get-timestamp) in its condition and tags. Every row is then evaluated
again each time the timestamp is updated, by default every hour, so an issue that depends on the time is raised or
resolved with the first checkpoint after the update:

```SQL
CHECK_TRUE(expires_at > gettimestamp(), 'Order {orderkey} has expired', orderkey => orderkey)
```

## Unsupported contexts

The following uses are rejected with a `NotSupportedException` when the plan is optimized or the stream is built.
Move the check into the `SELECT` list or `WHERE` clause of a query with a single input instead.

* A check in a join condition that uses columns from both sides of the join.
* A check in `VALUES`, or in a `SELECT` without `FROM`.
* A check in the arguments of a table function without an input, or in a table function join condition that uses
  the table function output.
* A check in the skip condition of an iteration (recursive query).
* A check in a window frame bound, or in the default argument of `LEAD` or `LAG`.
* A check inside the tag arguments of another check.

A message that is not a string literal is rejected the same way, see
[The message is the check name](#the-message-is-the-check-name).

Check functions require the column store, which is the default. A stream that runs in row mode fails to build when
the plan contains a check.

## Upgrading from earlier versions

Before this version, check functions called the listeners directly every time a failing row was evaluated, and
issues were never resolved. These changes require action when upgrading:

* The message must be a string literal. A computed message, such as `concat('User ', userkey, ' is invalid')`, fails
  the plan with a `NotSupportedException`. Write the row values as tags and reference them in the message instead:
  `'User {userkey} is invalid', userkey`.
* `CheckFailureNotification.Message` is renamed to `CheckName` and holds the message as written, with the `{tag}`
  placeholders not filled in. Fill them in from `Tags` where a rendered text is needed.
* `ICheckFailureListener` has the new required members `OnCheckResolved` and `OnCheckReset`.
* The `CheckFailureNotification` constructor takes the check id and the check name.
* `ICheckStatusListener` is new, for the pass or fail status of every check. `WithCheckLogger` and
  `WriteCheckFailuresToLogger` now also log the status of every check at information level.
* The check logger writes each kind of entry with its own [event id](#listener-interfaces) instead of event id 0,
  and the message template of a failure now starts with `Check failed: `.
* `ICheckNotificationReceiver`, `IFunctionServices.CheckNotificationReceiver` and `SetCheckNotificationReceiver` on
  `FunctionServices` and `FunctionsRegister` are removed, custom functions can no longer raise check failures.
* The plan hash changes for every plan that contains a check function. A stream with existing state and a check
  function fails to start with a plan hash mismatch until its state is reset or it starts on a new stream version,
  see the versioning options in [State Persistence](../../statepersistence.md).
