// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Storage.Memory;
using System.Globalization;
using System.Reflection;
using System.Text;
using EngineStream = FlowtideDotNet.Base.Engine.DataflowStream;

namespace FlowtideDotNet.Benchmarks.LateMaterialization
{
    /// <summary>
    /// Runs every workload small and compares the sink with the generator.
    /// </summary>
    internal static class LmSelfCheck
    {
        private const double Scale = 0.0005;
        private const int Rounds = 2;
        private const long Seed = 123;

        // 7,919 groups once made every group move a no-op
        private const double PrimeGroupsScale = 0.00197975;
        private const long PrimeGroups = 7919;

        public static async Task<int> Run(string only)
        {
            int failed = 0;
            bool first = true;
            for (int i = 0; i < LmWorkload.Names.Length; i++)
            {
                string name = LmWorkload.Names[i];
                if (only != "all" && !only.Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }
                var problems = new List<string>();
                try
                {
                    var wide = await RunOne(name, true, false, problems);
                    var narrow = await RunOne(name, false, false, problems);
                    if (narrow.TotalStateBytes >= wide.TotalStateBytes)
                    {
                        problems.Add($"narrow state {narrow.TotalStateBytes} is not smaller than wide state {wide.TotalStateBytes}");
                    }
                    var workload = LmWorkload.Create(name, Scale);
                    if (workload.HostedSql.Length > 0)
                    {
                        // Row stores in the sources, the fetch in the sink
                        await RunOne(name, true, false, problems, true);
                    }
                    if (workload.KeptOnceClass != null && LmProgram.SmallestTreeDifference(wide.Census!, narrow.Census!, workload.KeptOnceClass, out _) <= 0)
                    {
                        problems.Add($"no tree of class {workload.KeptOnceClass} holds wide data to keep once");
                    }
                    if (first)
                    {
                        // A shuffled load must give the same rows
                        await RunOne(name, true, true, problems);
                        await CheckCounters(name, problems);
                        await CheckCheckpoints(name, problems);
                        await CheckStop(problems);
                        first = false;
                    }
                    CheckGenerator(workload, problems);
                    if (workload.Fact.HasKind(LmColumnKind.GroupId))
                    {
                        var prime = LmWorkload.Create(name, PrimeGroupsScale);
                        if (prime.Fact.Columns[prime.Fact.GetColumnIndex(prime.GroupColumn!)].Modulus != PrimeGroups)
                        {
                            problems.Add($"scale {PrimeGroupsScale} no longer gives {PrimeGroups} groups");
                        }
                        CheckGenerator(prime, problems);
                    }
                }
                catch (Exception e)
                {
                    problems.Add(e.ToString());
                }
                Console.WriteLine(problems.Count == 0 ? $"PASS {name}" : $"FAIL {name}");
                for (int p = 0; p < problems.Count; p++)
                {
                    Console.WriteLine($"  {problems[p]}");
                }
                failed += problems.Count == 0 ? 0 : 1;
            }
            return failed == 0 ? 0 : 1;
        }

        private static async Task<LmRunResult> RunOne(string name, bool wide, bool shuffled, List<string> problems, bool hosted = false)
        {
            string form = (hosted ? "hosted" : wide ? "wide" : "narrow") + (shuffled ? " shuffled" : string.Empty);
            var workload = LmWorkload.Create(name, Scale);
            var control = new LmControl(Seed, shuffled, true, workload.Tables);
            var result = await LmRun.Run(new LmRunOptions()
            {
                Workload = name,
                Wide = wide,
                Hosted = hosted,
                Shuffled = shuffled,
                Scale = Scale,
                Rounds = Rounds,
                Seed = Seed,
                // The census storage cannot hold a row store's pages
                Census = !hosted,
                TrackRows = true,
                TimeoutSeconds = 180
            }, control);
            int before = problems.Count;
            if (!hosted)
            {
                CheckCensus(result, problems);
                CheckShape(workload, result, problems);
            }
            CheckWork(workload, result, problems);
            if (workload.GroupColumn == null)
            {
                CheckJoinRows(workload, control, problems);
            }
            else
            {
                CheckGroupRows(workload, control, problems);
            }
            if (!hosted)
            {
                // A hosted sink gets references, its rows are checked above
                CheckWeights(control, problems);
            }
            for (int i = before; i < problems.Count; i++)
            {
                problems[i] = $"{form}: {problems[i]}";
            }
            return result;
        }

        /// <summary>
        /// Every row the sink holds must have arrived exactly once, net.
        /// </summary>
        private static void CheckWeights(LmControl control, List<string> problems)
        {
            var weights = control.SinkWeights!;
            int wrong = 0;
            foreach (var row in control.SinkRows!)
            {
                weights.TryGetValue(LmControl.RowHash(row.Value), out var weight);
                if (weight != 1 && wrong++ < 3)
                {
                    problems.Add($"sink row {row.Key} arrived with a net weight of {weight}, expected 1");
                }
            }
            if (weights.Count != control.SinkRows.Count)
            {
                problems.Add($"{weights.Count} rows reached the sink with a net weight, it holds {control.SinkRows.Count}");
            }
        }

        private static async Task CheckCounters(string name, List<string> problems)
        {
            // A small cache must show evictions and reads
            var result = await LmRun.Run(new LmRunOptions()
            {
                Workload = name,
                Scale = Scale,
                Rounds = 1,
                Seed = Seed,
                CachePages = 64,
                TimeoutSeconds = 180
            });
            result.Counters.TryGetValue("flowtide_s3fifo_small_queue_evictions", out var evictions);
            result.Counters.TryGetValue("flowtide_cache_read_misses", out var misses);
            if (evictions <= 0 || misses <= 0)
            {
                problems.Add($"a run with a small cache reports {evictions} evictions and {misses} read misses");
            }
        }

        private static FieldInfo Hook(string name)
        {
            var context = typeof(EngineStream).Assembly.GetType("FlowtideDotNet.Base.Engine.Internal.StateMachine.StreamContext");
            var field = context?.GetField(name, BindingFlags.NonPublic | BindingFlags.Static);
            if (field == null)
            {
                throw new InvalidOperationException($"The engine has no test hook named {name}");
            }
            return field;
        }

        /// <summary>
        /// Two cases that once hung a run on a checkpoint.
        /// </summary>
        private static async Task CheckCheckpoints(string name, List<string> problems)
        {
            var workload = LmWorkload.Create(name, Scale);
            var control = new LmControl(Seed, false, false, workload.Tables);
            var scheduled = Hook("ScheduledCheckpointFiredHookForTests");
            var compaction = Hook("CompactionHookForTests");
            int early = 0;
            int cycles = 0;
            int held = 0;
            try
            {
                // No checkpoint timer may fire during the load
                scheduled.SetValue(null, new Action<string>(stream =>
                {
                    if (stream.StartsWith(LmRun.StreamPrefix, StringComparison.Ordinal) && !control.WaitForSink(0).IsCompleted)
                    {
                        Interlocked.Increment(ref early);
                    }
                }));
                // Each cycle's end waits for the next checkpoint request,
                // which then reaches the engine while the slot is taken
                compaction.SetValue(null, new Func<string, Task>(async stream =>
                {
                    if (!stream.StartsWith(LmRun.StreamPrefix, StringComparison.Ordinal))
                    {
                        return;
                    }
                    int next = Interlocked.Increment(ref cycles);
                    if (next > Rounds)
                    {
                        return;
                    }
                    var asked = control.WaitForCheckpointAsked(next);
                    if (await Task.WhenAny(asked, Task.Delay(TimeSpan.FromSeconds(30))) == asked)
                    {
                        Interlocked.Increment(ref held);
                    }
                }));
                await LmRun.Run(new LmRunOptions()
                {
                    Workload = name,
                    Scale = Scale,
                    Rounds = Rounds,
                    Seed = Seed,
                    TimeoutSeconds = 60
                }, control);
            }
            finally
            {
                scheduled.SetValue(null, null);
                compaction.SetValue(null, null);
            }
            if (early > 0)
            {
                problems.Add($"{early} scheduled checkpoints fired before the sink had the load");
            }
            if (held != Rounds)
            {
                problems.Add($"{held} of {Rounds} checkpoints were asked for while the one before still ran, the check proved nothing");
            }
        }

        /// <summary>
        /// A stop that fails is reported and never thrown.
        /// </summary>
        private static async Task CheckStop(List<string> problems)
        {
            var limit = TimeSpan.FromMilliseconds(50);
            string? done = await LmRun.StopFailure(Task.CompletedTask, limit);
            string? faulted = await LmRun.StopFailure(Task.FromException(new IOException("disk gone")), limit);
            string? cancelled = await LmRun.StopFailure(Task.FromCanceled(new CancellationToken(true)), limit);
            string? late = await LmRun.StopFailure(new TaskCompletionSource().Task, limit);
            if (done != null || faulted != "disk gone" || cancelled == null || late == null || cancelled == late)
            {
                problems.Add($"the four stop outcomes are reported as '{done}', '{faulted}', '{cancelled}' and '{late}'");
            }
        }

        private static void CheckCensus(LmRunResult result, List<string> problems)
        {
            long attributed = 0;
            var classes = LmCensus.ByClass(result.Census!);
            foreach (var entry in classes)
            {
                attributed += entry.Value;
            }
            if (attributed != result.TotalStateBytes)
            {
                problems.Add($"census classes hold {attributed} bytes, the wrapper counted {result.TotalStateBytes}");
            }
            // The storage itself must hold every counted page at its counted size
            problems.AddRange(result.CensusProblems!);
            if (classes[LmCensus.Source] == 0 || classes[LmCensus.Sink] == 0 || classes[LmCensus.Metadata] == 0)
            {
                problems.Add("source, sink or stream metadata state is empty");
            }
            if (classes[LmCensus.Unattributed] != 0)
            {
                problems.Add($"{classes[LmCensus.Unattributed]} bytes have no named tree");
            }
        }

        private static void CheckShape(LmWorkload workload, LmRunResult result, List<string> problems)
        {
            var counts = new Dictionary<string, int>();
            for (int i = 0; i < result.Nodes.Count; i++)
            {
                string displayName = result.Nodes[i].Substring(result.Nodes[i].IndexOf('|') + 1);
                counts[displayName] = counts.TryGetValue(displayName, out var count) ? count + 1 : 1;
            }
            foreach (var expected in workload.Shape)
            {
                counts.TryGetValue(expected.Key, out var actual);
                if (actual != expected.Value)
                {
                    problems.Add($"plan has {actual} operators named {expected.Key}, expected {expected.Value}");
                }
            }
            var classes = LmCensus.ByClass(result.Census!);
            if (workload.Shape.ContainsKey("Multi Join") && classes[LmCensus.JoinInput] == 0)
            {
                problems.Add("no join input state was found");
            }
            if (workload.Shape.TryGetValue("Multi Join", out var multiJoins) && multiJoins > 1 && classes[LmCensus.JoinIntermediate] == 0)
            {
                problems.Add("no stored rows between the multi joins were found");
            }
            if (workload.Shape.ContainsKey("Aggregation") && classes[LmCensus.AggregateMain] == 0)
            {
                problems.Add("no aggregate state was found");
            }
            if (workload.Parallelization > 1)
            {
                CheckPartitions(workload, result, counts, problems);
            }
        }

        private static void CheckPartitions(LmWorkload workload, LmRunResult result, Dictionary<string, int> counts, List<string> problems)
        {
            // Every aggregate partition reads an exchange and feeds the union
            counts.TryGetValue("Set(UnionAll)", out var unions);
            if (unions == 0)
            {
                problems.Add("the partitions are not merged by a union");
            }
            var names = new Dictionary<string, string>();
            for (int i = 0; i < result.Nodes.Count; i++)
            {
                int split = result.Nodes[i].IndexOf('|');
                names[result.Nodes[i].Substring(0, split)] = result.Nodes[i].Substring(split + 1);
            }
            int fromExchange = 0;
            int toUnion = 0;
            for (int i = 0; i < result.Edges.Count; i++)
            {
                int split = result.Edges[i].IndexOf('>');
                string source = names[result.Edges[i].Substring(0, split)];
                string target = names[result.Edges[i].Substring(split + 1)];
                if (target == "Aggregation" && (source == "Exchange" || source == "Partitioned Output"))
                {
                    fromExchange++;
                }
                if (source == "Aggregation" && target == "Set(UnionAll)")
                {
                    toUnion++;
                }
            }
            if (fromExchange != workload.Parallelization || toUnion != workload.Parallelization)
            {
                problems.Add($"{fromExchange} aggregates read an exchange and {toUnion} feed the union, expected {workload.Parallelization} of each");
            }
        }

        private static void CheckWork(LmWorkload workload, LmRunResult result, List<string> problems)
        {
            // Every kind of change must happen, or the check proves nothing
            long[] kinds = new long[3];
            long changed = 0;
            for (int t = 0; t < workload.Tables.Count; t++)
            {
                var table = workload.Tables[t];
                for (int round = 1; round <= Rounds; round++)
                {
                    for (long row = 0; row < table.Rows; row++)
                    {
                        if (LmData.IsDeleted(Seed, table, row, round - 1))
                        {
                            continue;
                        }
                        bool any = false;
                        for (int kind = 0; kind < 3; kind++)
                        {
                            if (LmData.IsSelected(Seed, table, kind, round, row))
                            {
                                kinds[kind]++;
                                any = true;
                            }
                        }
                        changed += any ? 1 : 0;
                    }
                }
            }
            if (kinds[LmData.PayloadUpdate] == 0 || kinds[LmData.InlineUpdate] == 0 || kinds[LmData.Delete] == 0)
            {
                problems.Add($"the rounds hold {kinds[0]} payload updates, {kinds[1]} inline updates and {kinds[2]} deletes, each must be above zero");
            }
            if (result.ChurnRows != changed)
            {
                problems.Add($"the sources sent {result.ChurnRows} changed rows, the generator gives {changed}");
            }
        }

        private static void CheckJoinRows(LmWorkload workload, LmControl control, List<string> problems)
        {
            var fact = workload.Fact;
            var rows = control.SinkRows!;
            long live = 0;
            int reported = 0;
            for (long row = 0; row < fact.Rows; row++)
            {
                if (LmData.IsDeleted(Seed, fact, row, Rounds))
                {
                    continue;
                }
                live++;
                if (!rows.TryGetValue(row.ToString(CultureInfo.InvariantCulture) + "|", out var actual))
                {
                    if (reported++ < 3)
                    {
                        problems.Add($"fact row {row} is missing in the sink");
                    }
                    continue;
                }
                for (int c = 0; c < control.SinkColumns.Count; c++)
                {
                    string? expected = ExpectedJoinValue(workload, control.SinkColumns[c], row);
                    if (expected == null)
                    {
                        if (reported++ < 3)
                        {
                            problems.Add($"sink column {control.SinkColumns[c]} has no expected value");
                        }
                    }
                    else if (expected != actual[c] && reported++ < 3)
                    {
                        problems.Add($"fact row {row} column {control.SinkColumns[c]} is {Short(actual[c])}, expected {Short(expected)}");
                    }
                }
            }
            if (rows.Count != live)
            {
                problems.Add($"sink holds {rows.Count} rows, expected {live}");
            }
        }

        private static string? ExpectedJoinValue(LmWorkload workload, string outputColumn, long factRow)
        {
            if (workload.Computed.TryGetValue(outputColumn, out var computed))
            {
                // Built from the expected values of its parts
                var text = new StringBuilder(computed.IsStruct ? "{" : string.Empty);
                for (int i = 0; i < computed.Parts.Length; i++)
                {
                    text.Append(ExpectedJoinValue(workload, computed.Parts[i], factRow));
                    if (computed.IsStruct)
                    {
                        text.Append(';');
                    }
                }
                return computed.IsStruct ? text.Append('}').ToString() : text.ToString();
            }
            // A plain name is a fact column, a prefixed one a dimension's
            var table = workload.Fact;
            long row = factRow;
            string column = outputColumn;
            int split = outputColumn.IndexOf('_');
            if (split > 0 && HasTable(workload, outputColumn.Substring(0, split)))
            {
                table = workload.GetTable(outputColumn.Substring(0, split));
                column = outputColumn.Substring(split + 1);
                row = LmData.ReferenceValue(Seed, workload.Fact, workload.Fact.GetColumnIndex(table.Name + "_id"), factRow);
            }
            for (int i = 0; i < table.Columns.Count; i++)
            {
                if (!table.Columns[i].Name.Equals(column, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }
                switch (table.Columns[i].Kind)
                {
                    case LmColumnKind.Items:
                        return LmData.Format(LmData.ItemsValue(Seed, table, i, row, Rounds));
                    case LmColumnKind.Text:
                    case LmColumnKind.GroupText:
                        return Encoding.UTF8.GetString(LmData.TextValue(Seed, table, i, row, Rounds));
                    default:
                        return LmData.LongValue(Seed, table, i, row, Rounds).ToString(CultureInfo.InvariantCulture);
                }
            }
            return null;
        }

        private static bool HasTable(LmWorkload workload, string name)
        {
            for (int i = 0; i < workload.Tables.Count; i++)
            {
                if (workload.Tables[i].Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
            return false;
        }

        private sealed class GroupValues
        {
            public long Count;
            public long Sum;
            public long Min = long.MaxValue;
            public long DimensionSum;
            public readonly HashSet<long> Distinct = new HashSet<long>();
        }

        private static void CheckGroupRows(LmWorkload workload, LmControl control, List<string> problems)
        {
            var fact = workload.Fact;
            int amount = fact.GetColumnIndex("amount");
            var groups = new Dictionary<long, GroupValues>();
            for (long row = 0; row < fact.Rows; row++)
            {
                if (LmData.IsDeleted(Seed, fact, row, Rounds))
                {
                    continue;
                }
                long group = workload.GroupOf(Seed, row, Rounds);
                if (!groups.TryGetValue(group, out var values))
                {
                    values = new GroupValues();
                    groups.Add(group, values);
                }
                long value = LmData.LongValue(Seed, fact, amount, row, Rounds);
                values.Count++;
                values.Sum += value;
                values.Min = Math.Min(values.Min, value);
                values.Distinct.Add(value);
                if (workload.Tables.Count > 1)
                {
                    // The group is a row of the dimension
                    var dimension = workload.Tables[1];
                    values.DimensionSum += LmData.LongValue(Seed, dimension, dimension.GetColumnIndex("a1"), group, Rounds);
                }
            }
            // Whole rows are compared, the group key included
            var columns = control.SinkColumns;
            var expected = new List<string>();
            var text = new StringBuilder();
            foreach (var group in groups)
            {
                text.Clear();
                for (int c = 0; c < columns.Count; c++)
                {
                    text.Append(ExpectedGroupValue(workload, columns[c], group.Key, group.Value)).Append('|');
                }
                expected.Add(text.ToString());
            }
            var actual = new List<string>();
            foreach (var row in control.SinkRows!)
            {
                text.Clear();
                for (int c = 0; c < row.Value.Length; c++)
                {
                    text.Append(row.Value[c]).Append('|');
                }
                actual.Add(text.ToString());
            }
            expected.Sort(StringComparer.Ordinal);
            actual.Sort(StringComparer.Ordinal);
            if (expected.Count != actual.Count)
            {
                problems.Add($"sink holds {actual.Count} groups, expected {expected.Count}");
                return;
            }
            for (int i = 0; i < expected.Count; i++)
            {
                if (expected[i] != actual[i])
                {
                    problems.Add($"group rows differ, sink has {Around(actual[i], expected[i])}, expected {Around(expected[i], actual[i])}");
                    return;
                }
            }
        }

        private static string ExpectedGroupValue(LmWorkload workload, string column, long group, GroupValues values)
        {
            switch (column)
            {
                case "c":
                    return values.Count.ToString(CultureInfo.InvariantCulture);
                case "s":
                    return values.Sum.ToString(CultureInfo.InvariantCulture);
                case "mn":
                    return values.Min.ToString(CultureInfo.InvariantCulture);
                case "cd":
                    return values.Distinct.Count.ToString(CultureInfo.InvariantCulture);
                case "da":
                    return values.DimensionSum.ToString(CultureInfo.InvariantCulture);
            }
            // A key column of the dimension or the group
            var table = workload.Tables.Count > 1 ? workload.Tables[1] : workload.Fact;
            int index = table.GetColumnIndex(column);
            switch (table.Columns[index].Kind)
            {
                case LmColumnKind.Key:
                case LmColumnKind.GroupId:
                    return group.ToString(CultureInfo.InvariantCulture);
                case LmColumnKind.GroupText:
                    return Encoding.UTF8.GetString(LmData.GroupText(Seed, table, index, group));
                case LmColumnKind.Text:
                    return Encoding.UTF8.GetString(LmData.TextValue(Seed, table, index, group, Rounds));
                default:
                    throw new InvalidOperationException($"Sink column {column} is not a group key or a measure");
            }
        }

        /// <summary>
        /// Where a long row first differs from another.
        /// </summary>
        private static string Around(string value, string other)
        {
            int at = 0;
            while (at < value.Length && at < other.Length && value[at] == other[at])
            {
                at++;
            }
            int start = Math.Max(0, at - 12);
            return $"...{value.Substring(start, Math.Min(36, value.Length - start))}... at {at}";
        }

        private static string Short(string? value)
        {
            if (value == null)
            {
                return "nothing";
            }
            return value.Length > 24 ? value.Substring(0, 24) + "..." : value;
        }

        private static string? Value(LmTable table, int column, long row, int round)
        {
            switch (table.Columns[column].Kind)
            {
                case LmColumnKind.Text:
                case LmColumnKind.GroupText:
                    return Encoding.UTF8.GetString(LmData.TextValue(Seed, table, column, row, round));
                case LmColumnKind.Items:
                    return LmData.Format(LmData.ItemsValue(Seed, table, column, row, round));
                default:
                    return LmData.LongValue(Seed, table, column, row, round).ToString(CultureInfo.InvariantCulture);
            }
        }

        private static void CheckGenerator(LmWorkload workload, List<string> problems)
        {
            for (int t = 0; t < workload.Tables.Count; t++)
            {
                var table = workload.Tables[t];
                // Rows at both ends and rows the rounds change
                var rows = new List<long>() { 0, 1, table.Rows - 1 };
                for (long row = 0; row < table.Rows && rows.Count < 8; row++)
                {
                    if (LmData.IsSelected(Seed, table, LmData.PayloadUpdate, 1, row) || LmData.IsSelected(Seed, table, LmData.InlineUpdate, 1, row))
                    {
                        rows.Add(row);
                    }
                }
                var all = new List<int>();
                for (int c = 0; c < table.Columns.Count; c++)
                {
                    all.Add(c);
                }
                using var first = LmData.CreateBatch(Seed, table, all, rows, Rounds, GlobalMemoryManager.Instance);
                using var second = LmData.CreateBatch(Seed, table, all, rows, Rounds, GlobalMemoryManager.Instance);
                using var other = LmData.CreateBatch(Seed + 1, table, all, rows, Rounds, GlobalMemoryManager.Instance);
                bool differs = false;
                for (int c = 0; c < table.Columns.Count; c++)
                {
                    for (int r = 0; r < rows.Count; r++)
                    {
                        var value = first.Columns[c].GetValueAt(r, default);
                        if (DataValueComparer.Instance.Compare(value, second.Columns[c].GetValueAt(r, default)) != 0)
                        {
                            problems.Add($"table {table.Name} column {table.Columns[c].Name} differs between two builds with one seed");
                        }
                        if (DataValueComparer.Instance.Compare(value, other.Columns[c].GetValueAt(r, default)) != 0)
                        {
                            differs = true;
                        }
                    }
                }
                if (!differs)
                {
                    problems.Add($"table {table.Name} is the same for two seeds");
                }
                // A payload update must change a wide value
                for (long row = 0; row < table.Rows; row++)
                {
                    if (!LmData.IsSelected(Seed, table, LmData.PayloadUpdate, 1, row))
                    {
                        continue;
                    }
                    bool changed = false;
                    for (int c = 0; c < table.Columns.Count && !changed; c++)
                    {
                        changed = table.Columns[c].Payload && Value(table, c, row, 0) != Value(table, c, row, 1);
                    }
                    if (!changed)
                    {
                        problems.Add($"table {table.Name} with {table.Rows} rows: the payload update of row {row} changes nothing");
                        break;
                    }
                }
                // The shuffle must visit every row once
                var seen = new HashSet<long>();
                for (long index = 0; index < table.Rows; index++)
                {
                    seen.Add(LmData.RowAt(Seed, table, index, true));
                }
                if (seen.Count != table.Rows)
                {
                    problems.Add($"table {table.Name} shuffles {table.Rows} rows into {seen.Count}");
                }
            }
        }
    }
}
