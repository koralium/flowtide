---
sidebar_position: 10
---

# Check Operator

The *Check Operator* evaluates [check functions](../expressions/scalarfunctions/check.md) and keeps track of the
issues they raise and the counts of each check. It is not written by hand: the plan optimizer finds every
`check_value` and `check_true` call, moves it into a check operator on the input the call is evaluated against, and
replaces the call with its result (the value for `check_value`, the condition being `true` for `check_true`).

Rows pass through the operator unchanged. Rows that pass all checks only cost the evaluation of each check condition and
its guards, and are never copied. The operator implements the common *Emit* field, which reuses the input columns.

The operator is stateful. Per check it uses one persistent B+ tree keyed on the tag values, with the net number of
failing rows of each issue as the value. A check without tags uses a single constant key, so it has one issue. The
number of active issues and failing rows of every check is kept in memory, updated as rows are processed, and stored
as an object state at each checkpoint.

At each checkpoint the issue changes and the status of each check whose counts changed are handed to the stream,
which publishes them to the listeners once the checkpoint is committed. Every start publishes the active issues and
the status of every check from the restored state before new data is processed.

The work for each kind of listener is only done when such a listener is registered. With a check failure listener,
a second, temporary B+ tree per check collects the issues that became active or resolved since the last checkpoint.
With a check status listener, the counts are compared with the last reported status at each checkpoint. The counts and
the metrics are always maintained.

A deletion of a failing row that the operator has never seen, for example after an inconsistent source, is kept as a
negative count. It never makes an issue active and is not counted as a failing row, and a later insert of the same
row brings the count back to zero.

The operator is only supported with the column store.

## Check Relation

The operator is defined by a custom relation that uses `ExtensionSingleRel` in substrait, with the following message
in the detail field and the emit in the common field:

```
message CheckRelation {
    repeated Check checks = 1;

    message Check {
        // Raises an issue when it evaluates to boolean false.
        substrait.Expression condition = 1;

        // Check name template, placeholders like {tag} are left unrendered.
        string message = 2;

        repeated Tag tags = 3;

        // Must all hold before the check is evaluated, empty means always.
        repeated Guard guards = 4;
    }

    message Tag {
        string key = 1;
        substrait.Expression value = 2;
    }

    message Guard {
        substrait.Expression expression = 1;
        Kind kind = 2;

        enum Kind {
            // The expression is true, the CASE branch was taken.
            KIND_IS_TRUE = 0;
            // The expression is not true, an earlier CASE branch was not taken.
            KIND_IS_NOT_TRUE = 1;
            // The expression is null, an earlier COALESCE argument was null.
            KIND_IS_NULL = 2;
        }
    }
}
```

The condition, the tag values and the guards are evaluated against the input row of the relation. The message is the
name of the check and is never evaluated.

## Metrics

The *Check Operator* has the following metrics:

| Metric Name           | Type      | Description                                                  |
| --------------------- | --------- | ------------------------------------------------------------ |
| busy                  | Gauge     | Value 0-1 on how busy the operator is.                       |
| backpressure          | Gauge     | Value 0-1 on how much backpressure the operator has.         |
| health                | Gauge     | Value 0 or 1, if the operator is healthy or not.             |
| events                | Counter   | How many events that pass through the operator.              |
| events_processed      | Counter   | How many events the operator processes.                      |
| check_active_issues   | Gauge     | How many issues of a check are active, one value per check.  |
| check_failing_rows    | Gauge     | How many rows fail a check, one value per check.             |

`check_active_issues` and `check_failing_rows` have these labels on top of the standard labels:

| Label Name    | Description                                                                         |
| ------------- | ----------------------------------------------------------------------------------- |
| check_name    | The message of the check as written in the query, with the placeholders not filled in. |
| check_id      | The id of the check, `{operatorId}:{checkIndex}`, prefixed by the substream name in distributed mode. |

The tag values of the issues never become labels, so the number of series is the number of checks times the number of
partitions they run in, however many issues there are. This keeps the metrics safe to scrape with Prometheus. The
values follow the processed rows, so they can be ahead of what the listeners have received until the next checkpoint
is committed.

A check in a partitioned part of the plan has one series per partition. When every check has its own message,
summing `check_failing_rows` over them gives the failing rows of the check, for example
`sum by (check_name) (flowtide_check_failing_rows)`. `check_active_issues` does not add up over partitions: rows with
the same tag values can fail in several partitions, so one issue can be counted in several series, and a check without
tags counts one issue in every partition that has a failing row. A check passes when all its series are zero.

> [!NOTE]
> At this point, a check operator will never be unhealthy.
> If there is a failure against the state, the stream will instead restart.
