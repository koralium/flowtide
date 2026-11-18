# Late materialization baseline

Measuring tools for the late materialization work. They run from the benchmark project:

```
dotnet run --project tests/FlowtideDotNet.Benchmarks -c Release -f net10.0 -- --late-materialization <command> [options]
```

| Command | What it does |
| --- | --- |
| `selfcheck` | Runs every workload small and compares the sink's rows with the generator. |
| `describe` | Prints a workload's tables and its two queries. |
| `run` | Runs one workload once and prints one JSON line. |
| `census` | Runs a workload wide and narrow and prints stored bytes per state class. |
| `paired` | Runs `Off` and `Placebo` in separate processes and prints the drift. |

| Option | Meaning |
| --- | --- |
| `--workload` | `LM-J5`, `LM-J7`, `LM-X`, `LM-G`, `LM-GM`, `LM-FD`, `LM-P`, `LM-N`. `all` for `census` and `selfcheck`. |
| `--scale` | 1 is the full size, 20 million fact rows. |
| `--rounds` | Update rounds after the load. |
| `--narrow` | Run the narrow form. A flag takes no value, or `true` or `false`. |
| `--shuffled` | Load rows in a seeded shuffle instead of key order. |
| `--cache-pages` | Pages the state cache may hold. |
| `--state-dir` | Folder for the state. `run` and `paired` use a temporary folder when it is not given. |
| `--memory-state` | Keep the state in memory. Do not read memory numbers from such a run. |
| `--seed`, `--pairs`, `--timeout` | The data seed, the number of pairs, and the seconds a run may take. |

## What a run measures

- **Load:** from the start until the sink has uploaded the rows of every table.
- **Rounds:** each round changes 1% of every table's rows in their payload, 1% in their numbers, and deletes 0.1% of
  the fact rows. A round ends when the sink has uploaded it.
- **Checkpoints:** one after the load and one after each round. Each time runs from the moment the sink has the data
  to the commit that makes it durable.
  - The load checkpoint is the engine's own first checkpoint. The sources hold it back until the sink has the load.
  - A round's checkpoint is asked for by the run. If the checkpoint before it is still compacting, the engine puts
    the request off and the run asks again, so that wait is part of the time.
  - A run fails if a checkpoint commits at any other moment.
- **Counters:** the engine's cache evictions, cache read hits and misses, and storage reads and writes of this run
  only, read once at the end. Timed runs carry no other observer.

A process that runs several workloads keeps every finished stream in memory until it exits, because the engine does
not cancel a checkpoint timer it has replaced. Peak memory is therefore only reported by `run`, one stream per process.

`ChurnRows` counts the changed rows the sources sent. It is the same for every run of one seed, and `paired` reports
any pair where it is not. `GenerationSeconds` is the time the sources spent building rows and finding the changed
ones. That time is inside the load and round times.

## What the updates do in each workload

| Workload | Payload update | Number update |
| --- | --- | --- |
| `LM-J5`, `LM-J7`, `LM-X`, `LM-N` | New text in the row's wide columns. | New `k` values on facts, new `a1` on dimensions. |
| `LM-G`, `LM-GM` | The row moves to another group. | New `amount`. |
| `LM-FD` | New name, email and address of a user, which is the wide group key. | New `amount` on orders, new `a1` on users. |
| `LM-P` | New wide group key of a group row. | New `amount` on facts, new `a1` on group rows. |

A change to a column the query does not read is dropped by the source, as in any stream.

## How to read the census

The narrow run is the same query without its wide columns, so wide minus narrow is the wide data a state class holds.
`LM-X` is the exception: its narrow form only drops the computed columns. Its share is what a store after the
projection could remove, on top of what `LM-J7` shows for the source columns.

Some of that wide data would stay with late materialization, and the report marks those classes:

| Workload | Classes that stay | Why |
| --- | --- | --- |
| `LM-J5`, `LM-J7`, `LM-N`, `LM-FD` | source | The source's copy becomes the stored copy. |
| `LM-X` | source, one copy in join intermediate | Computed columns have no source copy. Two join trees and the sink hold them, and one copy stays. The report takes the smallest of the two trees as that copy and names it. |
| `LM-G`, `LM-GM`, `LM-P` | source, aggregate main, sink | The group key stays once as the stored copy and inline as the sink's key. |

Everything else counts as removable. The share is an upper bound: it leaves out the references and any index the
feature would add.

The census wrapper serializes every page a second time to learn its size. Never take timings from a census run.
`census` also compares its pages with the storage's own page map, in both directions, and exits with 1 when they
differ.

## Arms

`paired` sets `FLOWTIDE_LM_ARM` for each child process. Half of the pairs start with each arm. Both arms run the same
code today, so the drift it prints is the noise of the method on this machine. Drift is the mean log ratio in percent,
with a 95% interval.

## Regression controls

These existing benchmarks are the controls for changes to shared paths:

```
dotnet run --project tests/FlowtideDotNet.Nexmark -c Release -f net10.0 -- --filter "*Query3*" "*Query5*" "*Query8*"
dotnet run --project tests/FlowtideDotNet.Benchmarks -c Release -f net10.0 -- --filter "*ColumnStoreTreeBenchmark*" "*BPlusTreeBulkInsertBenchmark*" "*BPlusTreeBulkSearchBenchmark*" "*BatchSortBenchmark*"
```

`LM-J5` and `LM-J7` are the multi join controls.
