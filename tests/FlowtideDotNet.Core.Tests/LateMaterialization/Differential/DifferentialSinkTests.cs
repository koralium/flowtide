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

using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.LateMaterialization.Differential
{
    public class DifferentialSinkTests
    {
        // k, n, text, note; the last two stored
        private static ReadRelation Read(string table, bool hosted)
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = [table] },
                BaseSchema = new NamedStruct()
                {
                    Names = ["k", "n", "text", "note"],
                    Struct = new Struct() { Types = [new AnyType(), new AnyType(), new AnyType(), new AnyType()] }
                },
                RowStore = hosted ? new RowStoreDefinition() { Name = table, StoredColumns = [2, 3] } : null
            };
        }

        private static DirectFieldReference Field(int field)
        {
            return new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = field } };
        }

        private static Plan Write(Relation input, int columns)
        {
            return new Plan()
            {
                Relations =
                [
                    new WriteRelation()
                    {
                        Input = input,
                        NamedObject = new NamedTable() { Names = ["output"] },
                        TableSchema = new NamedStruct() { Names = Enumerable.Range(0, columns).Select(x => "c" + x).ToList() }
                    }
                ]
            };
        }

        private static DifferentialTable Table(string name, params object?[][] rows)
        {
            var table = new DifferentialTable(name, 4);
            table.Load(rows);
            return table;
        }

        private static object?[] Row(long key, long? number, string? text, string? note)
        {
            return [key, number, text, note];
        }

        // The table as it is, note before text
        private static Plan Copy(bool hosted)
        {
            Relation input = Read("t", hosted);
            if (!hosted)
            {
                input.Emit = [0, 1, 3, 2];
                return Write(input, 4);
            }
            return Write(new FetchRequiredColumnsRelation()
            {
                Input = input,
                Fetches = [new RowStoreFetch() { ReferenceColumn = 2, StoreName = "t", Columns = [1, 0] }],
                Emit = [0, 1, 3, 4]
            }, 4);
        }

        private static int Count(DifferentialStream stream, string displayName)
        {
            return stream.Stream.GetDiagnosticsGraph().Nodes.Values.Count(x => x.DisplayName == displayName);
        }

        [Theory]
        [InlineData(ExecutionMode.OnCheckpoint)]
        [InlineData(ExecutionMode.OnWatermark)]
        [InlineData(ExecutionMode.Hybrid)]
        public async Task ASinkFetchesTheRowsItUploads(ExecutionMode mode)
        {
            await using var pair = new DifferentialPair(
                Copy,
                () => [Table("t", Row(1, 5, "a", "x"), Row(2, 20, "b", null), Row(3, 30, null, "z"))],
                () => new DifferentialDestination() { Mode = mode });
            await pair.StartAsync();
            await pair.Expect("1|5|x|a", "2|20|null|b", "3|30|z|null");
            Assert.Equal(0, Count(pair.Hosted, "Fetch required columns"));

            pair.Change(tables => tables["t"].Upsert(Row(1, 6, "a2", "x"), Row(4, 40, "d", "w")));
            pair.Change(tables => tables["t"].Delete(2));
            await pair.Expect("1|6|x|a2", "3|30|z|null", "4|40|w|d");
            Assert.Equal(pair.Plain.Destination!.Operations, pair.Hosted.Destination!.Operations);

            await pair.RestartAsync();
            pair.Change(tables => tables["t"].Upsert(Row(3, 30, "c", "z")));
            pair.Change(tables => tables["t"].Delete(1, 4));
            await pair.Expect("3|30|z|c");
            Assert.Equal(pair.Plain.Destination!.Operations, pair.Hosted.Destination!.Operations);
        }

        // l joined with r on l.n = r.k, keyed by l.k
        private static Plan Join(bool hosted)
        {
            var left = Read("l", hosted);
            var join = new MergeJoinRelation()
            {
                Left = left,
                Right = Read("r", hosted),
                Type = JoinType.Left,
                LeftKeys = [Field(1)],
                RightKeys = [Field(left.OutputLength)]
            };
            if (!hosted)
            {
                return Write(join, 8);
            }
            return Write(new FetchRequiredColumnsRelation()
            {
                Input = join,
                Fetches =
                [
                    new RowStoreFetch() { ReferenceColumn = 2, StoreName = "l", Columns = [0, 1] },
                    new RowStoreFetch() { ReferenceColumn = 5, StoreName = "r", Columns = [0, 1] }
                ],
                Emit = [0, 1, 6, 7, 3, 4, 8, 9]
            }, 8);
        }

        [Theory]
        [InlineData(ExecutionMode.OnCheckpoint)]
        [InlineData(ExecutionMode.OnWatermark)]
        public async Task ASinkFetchesFromTwoStoresBehindAJoin(ExecutionMode mode)
        {
            await using var pair = new DifferentialPair(
                Join,
                () =>
                [
                    Table("l", Row(1, 10, "l1", "a"), Row(2, 10, "l2", null), Row(3, 30, "l3", "c")),
                    Table("r", Row(10, 1, "r10", "x"), Row(20, 2, "r20", "y"))
                ],
                () => new DifferentialDestination() { Mode = mode });
            await pair.StartAsync();
            await pair.Expect("1|10|l1|a|10|1|r10|x", "2|10|l2|null|10|1|r10|x", "3|30|l3|c|null|null|null|null");
            Assert.Equal(0, Count(pair.Hosted, "Fetch required columns"));

            pair.Change(tables => tables["r"].Upsert(Row(10, 1, "r10b", "x"), Row(30, 3, "r30", null)));
            pair.Change(tables => tables["l"].Delete(2));
            await pair.Expect("1|10|l1|a|10|1|r10b|x", "3|30|l3|c|30|3|r30|null");

            await pair.RestartAsync();
            pair.Change(tables => tables["r"].Delete(10));
            pair.Change(tables => tables["l"].Upsert(Row(4, 20, "l4", "d")));
            await pair.Expect("1|10|l1|a|null|null|null|null", "3|30|l3|c|30|3|r30|null", "4|20|l4|d|20|2|r20|y");
        }

        [Theory]
        [InlineData(ExecutionMode.OnCheckpoint)]
        [InlineData(ExecutionMode.OnWatermark)]
        public async Task TheFirstUploadIsComparedWithWhatTheDestinationHolds(ExecutionMode mode)
        {
            await using var pair = new DifferentialPair(
                Copy,
                () => [Table("t", Row(1, 5, "a", "x"), Row(2, 20, "b", null), Row(3, 30, null, "z"))],
                () => new DifferentialDestination()
                {
                    Mode = mode,
                    Existing = [[1L, 5L, "x", "a"], [2L, 20L, null, "old"], [9L, 90L, "gone", "gone"]]
                });
            await pair.StartAsync();
            await pair.Expect("2|20|null|b", "3|30|z|null");
            Assert.Equal(["delete 9", "upsert 2|20|null|b", "upsert 3|30|z|null"], pair.Hosted.Destination!.Operations.Order(StringComparer.Ordinal));
            Assert.Equal(pair.Plain.Destination!.Operations, pair.Hosted.Destination!.Operations);

            pair.Change(tables => tables["t"].Upsert(Row(1, 6, "a2", "x")));
            pair.Change(tables => tables["t"].Delete(3));
            await pair.Expect("1|6|x|a2", "2|20|null|b");
            Assert.Equal(pair.Plain.Destination!.Operations, pair.Hosted.Destination!.Operations);
        }

        [Theory]
        [InlineData(ExecutionMode.OnWatermark)]
        [InlineData(ExecutionMode.Hybrid)]
        public async Task ASinkThatSendsInTheBackgroundFetchesBeforeItHandsTheRowsOver(ExecutionMode mode)
        {
            await using var pair = new DifferentialPair(
                Copy,
                () => [Table("t", Enumerable.Range(0, 50).Select(x => Row(x, x, "t" + x, x % 3 == 0 ? null : "n" + x)).ToArray())],
                () => new DifferentialDestination() { Mode = mode, Background = true });
            await pair.StartAsync();
            await pair.Expect(Enumerable.Range(0, 50).Select(x => $"{x}|{x}|{(x % 3 == 0 ? "null" : "n" + x)}|t{x}").ToArray());

            for (int round = 0; round < 5; round++)
            {
                int offset = round;
                pair.Change(tables => tables["t"].Upsert(Enumerable.Range(0, 10).Select(x => Row(x + offset, 100 + offset, "r" + offset, "m" + x)).ToArray()));
                pair.Change(tables => tables["t"].Delete(40 + offset));
            }
            var expected = new Dictionary<long, string>();
            for (int x = 0; x < 50; x++)
            {
                expected[x] = $"{x}|{x}|{(x % 3 == 0 ? "null" : "n" + x)}|t{x}";
            }
            for (int round = 0; round < 5; round++)
            {
                for (int x = 0; x < 10; x++)
                {
                    expected[x + round] = $"{x + round}|{100 + round}|m{x}|r{round}";
                }
                expected.Remove(40 + round);
            }
            await pair.Expect(expected.Values.ToArray());
        }

        [Fact]
        public async Task WideRowsAreFetchedInSmallerChunks()
        {
            await using var pair = new DifferentialPair(
                Copy,
                () => [Table("t", Enumerable.Range(0, 2200).Select(x => Row(x, x, new string((char)('a' + x % 26), 9000) + x, "n" + x)).ToArray())],
                () => new DifferentialDestination());
            await pair.StartAsync();
            await pair.Expect(Enumerable.Range(0, 2200).Select(x => $"{x}|{x}|n{x}|{new string((char)('a' + x % 26), 9000)}{x}").ToArray());

            pair.Change(tables => tables["t"].Delete(Enumerable.Range(0, 2100).Select(x => (long)x).ToArray()));
            await pair.Expect(Enumerable.Range(2100, 100).Select(x => $"{x}|{x}|n{x}|{new string((char)('a' + x % 26), 9000)}{x}").ToArray());
        }

        [Theory]
        [InlineData(ExecutionMode.OnCheckpoint)]
        [InlineData(ExecutionMode.OnWatermark)]
        public async Task AfterAFailureTheSinkWritesWhatThePlainSinkWrites(ExecutionMode mode)
        {
            await using var pair = new DifferentialPair(
                Join,
                () =>
                [
                    Table("l", Row(1, 10, "l1", "a"), Row(2, 10, "l2", null), Row(3, 30, "l3", "c")),
                    Table("r", Row(10, 1, "r10", "x"), Row(20, 2, "r20", "y"))
                ],
                () => new DifferentialDestination() { Mode = mode });
            await pair.StartAsync();
            await pair.Expect("1|10|l1|a|10|1|r10|x", "2|10|l2|null|10|1|r10|x", "3|30|l3|c|null|null|null|null");
            pair.Change(tables => tables["l"].Upsert(Row(3, 20, "l3b", "c")));
            await pair.Expect("1|10|l1|a|10|1|r10|x", "2|10|l2|null|10|1|r10|x", "3|20|l3b|c|20|2|r20|y");

            foreach (var stream in new[] { pair.Hosted, pair.Plain })
            {
                await stream.Stream.InjectFailureForTests(new InvalidOperationException("A failure for the test."));
                var until = DateTime.UtcNow.AddSeconds(30);
                while (stream.Stream.State != Base.Engine.StreamStateValue.Running && DateTime.UtcNow < until)
                {
                    await Task.Delay(25);
                }
                stream.Failures.Clear();
            }

            pair.Change(tables => tables["r"].Delete(10));
            pair.Change(tables => tables["l"].Upsert(Row(4, 20, "l4", null)));
            await pair.Expect("1|10|l1|a|null|null|null|null", "2|10|l2|null|null|null|null|null", "3|20|l3b|c|20|2|r20|y", "4|20|l4|null|20|2|r20|y");
            Assert.Equal(pair.Plain.Destination!.Operations.Order(StringComparer.Ordinal), pair.Hosted.Destination!.Operations.Order(StringComparer.Ordinal));
        }

        private static DifferentialTable Shared(string name, params object?[][] rows)
        {
            var table = new DifferentialTable(name, 4) { WatermarkName = "shared" };
            table.Load(rows);
            return table;
        }

        private static async Task FailAndRecover(DifferentialPair pair)
        {
            foreach (var stream in new[] { pair.Hosted, pair.Plain })
            {
                await stream.Stream.InjectFailureForTests(new InvalidOperationException("A failure for the test."));
                var until = DateTime.UtcNow.AddSeconds(30);
                while (stream.Stream.State != Base.Engine.StreamStateValue.Running && DateTime.UtcNow < until)
                {
                    await Task.Delay(25);
                }
                stream.Failures.Clear();
            }
        }

        [Fact]
        public async Task AfterACrashWithUnsentRowsTheSinkWritesWhatThePlainSinkWrites()
        {
            await using var pair = new DifferentialPair(
                Join,
                () =>
                [
                    Shared("l", Row(1, 10, "l1", "a"), Row(2, 10, "l2", null), Row(3, 20, "l3", "c")),
                    Shared("r", Row(10, 1, "r10", "x"), Row(20, 2, "r20", "y"))
                ],
                () => new DifferentialDestination() { Mode = ExecutionMode.OnWatermark });
            await pair.StartAsync();
            await pair.Expect("1|10|l1|a|10|1|r10|x", "2|10|l2|null|10|1|r10|x", "3|20|l3|c|20|2|r20|y");

            pair.Change(tables => tables["l"].Upsert(Row(1, 10, "l1 changed", "a")));
            await pair.SettleBoth();
            Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
            bool unsent = !pair.Plain.Result().Contains("1|10|l1 changed|a|10|1|r10|x");

            await FailAndRecover(pair);
            pair.Change(tables => tables["r"].Upsert(Row(20, 2, "r20 changed", "y")));
            await pair.SettleBoth();

            Assert.True(unsent, "The change of one read was sent although its watermark was held back.");
            Assert.Contains("3|20|l3|c|20|2|r20 changed|y", pair.Hosted.Result());
            Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
            Assert.Equal(pair.Plain.Destination!.Operations.Order(StringComparer.Ordinal), pair.Hosted.Destination!.Operations.Order(StringComparer.Ordinal));
        }

        [Fact]
        public async Task AfterACrashWithUnsentRowsTheRestoredRowsOfThatKeyAreFetched()
        {
            await using var pair = new DifferentialPair(
                Join,
                () =>
                [
                    Shared("l", Row(1, 10, "l1", "a"), Row(2, 10, "l2", null), Row(3, 20, "l3", "c")),
                    Shared("r", Row(10, 1, "r10", "x"), Row(20, 2, "r20", "y"))
                ],
                () => new DifferentialDestination() { Mode = ExecutionMode.OnWatermark });
            await pair.StartAsync();
            await pair.Expect("1|10|l1|a|10|1|r10|x", "2|10|l2|null|10|1|r10|x", "3|20|l3|c|20|2|r20|y");

            pair.Change(tables => tables["l"].Upsert(Row(1, 10, "l1 changed", "a")));
            await pair.SettleBoth();
            bool unsent = !pair.Plain.Result().Contains("1|10|l1 changed|a|10|1|r10|x");

            await FailAndRecover(pair);
            pair.Change(tables => tables["r"].Upsert(Row(10, 1, "r10 changed", "x")));
            await pair.SettleBoth();

            Assert.True(unsent);
            string[] expected = ["1|10|l1 changed|a|10|1|r10 changed|x", "2|10|l2|null|10|1|r10 changed|x", "3|20|l3|c|20|2|r20|y"];
            Assert.Equal(expected, pair.Plain.Result());
            Assert.Equal(expected, pair.Hosted.Result());
            Assert.Equal(pair.Plain.Destination!.Operations.Order(StringComparer.Ordinal), pair.Hosted.Destination!.Operations.Order(StringComparer.Ordinal));
        }

        [Theory]
        [InlineData(ExecutionMode.OnWatermark)]
        [InlineData(ExecutionMode.Hybrid)]
        public async Task ACheckpointWaitsForABackgroundSendThatIsUnderWay(ExecutionMode mode)
        {
            await using var pair = new DifferentialPair(
                Copy,
                () => [Table("t", Row(1, 5, "a", "x"), Row(2, 20, "b", null))],
                () => new DifferentialDestination() { Mode = mode, Background = true });
            await pair.StartAsync();
            await pair.Expect("1|5|x|a", "2|20|null|b");

            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            int entered = 0;
            foreach (var stream in new[] { pair.Hosted, pair.Plain })
            {
                stream.Destination!.SendEntered = () => Interlocked.Increment(ref entered);
                stream.Destination.SendGate = () => release.Task;
            }
            pair.Change(tables => tables["t"].Upsert(Row(1, 6, "a2", "x")));
            await pair.Hosted.TriggerAsync();
            await pair.Plain.TriggerAsync();
            var until = DateTime.UtcNow.AddSeconds(30);
            while (Volatile.Read(ref entered) < 2 && DateTime.UtcNow < until)
            {
                await Task.Delay(10);
            }
            Assert.Equal(2, Volatile.Read(ref entered));
            int hostedSaved = pair.Hosted.Completed;
            int plainSaved = pair.Plain.Completed;
            pair.Change(tables => tables["t"].Upsert(Row(2, 21, "b2", "y"), Row(3, 30, "c", null)));
            await pair.Hosted.TriggerAsync();
            await pair.Plain.TriggerAsync();
            await Task.Delay(400);

            Assert.Equal(hostedSaved, pair.Hosted.Completed);
            Assert.Equal(plainSaved, pair.Plain.Completed);
            Assert.Equal(["1|5|x|a", "2|20|null|b"], pair.Hosted.Result());

            release.SetResult();
            await pair.Expect("1|6|x|a2", "2|21|y|b2", "3|30|null|c");
            Assert.Equal(pair.Plain.Destination!.Operations.Order(StringComparer.Ordinal), pair.Hosted.Destination!.Operations.Order(StringComparer.Ordinal));
        }

        [Theory]
        [InlineData(ExecutionMode.OnWatermark)]
        [InlineData(ExecutionMode.Hybrid)]
        public async Task RowsThatWaitedForABackgroundSendAreSentWhenItEnds(ExecutionMode mode)
        {
            await using var pair = new DifferentialPair(
                Copy,
                () => [Table("t", Row(1, 5, "a", "x"), Row(2, 20, "b", null))],
                () => new DifferentialDestination() { Mode = mode, Background = true },
                builder => builder.SetMinimumTimeBetweenCheckpoint(TimeSpan.FromDays(1)));
            await pair.StartAsync();
            await pair.Expect("1|5|x|a", "2|20|null|b");

            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var sendsEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var firstWatermarks = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            int entered = 0;
            int processed = 0;
            foreach (var stream in new[] { pair.Hosted, pair.Plain })
            {
                stream.Destination!.SendEntered = () =>
                {
                    if (Interlocked.Increment(ref entered) == 2)
                    {
                        sendsEntered.TrySetResult();
                    }
                };
                stream.Destination.SendGate = () => release.Task;
                stream.Destination.WatermarkProcessed = () =>
                {
                    if (Interlocked.Increment(ref processed) == 2)
                    {
                        firstWatermarks.TrySetResult();
                    }
                };
            }
            try
            {
                pair.Change(tables => tables["t"].Upsert(Row(1, 6, "a2", "x")));
                await pair.Hosted.TriggerAsync();
                await pair.Plain.TriggerAsync();
                await Task.WhenAll(sendsEntered.Task, firstWatermarks.Task).WaitAsync(TimeSpan.FromSeconds(30));

                var deferredWatermarks = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                int deferred = 0;
                foreach (var stream in new[] { pair.Hosted, pair.Plain })
                {
                    stream.Destination!.WatermarkProcessed = () =>
                    {
                        if (Interlocked.Increment(ref deferred) == 2)
                        {
                            deferredWatermarks.TrySetResult();
                        }
                    };
                }
                pair.Change(tables => tables["t"].Upsert(Row(2, 21, "b2", "y"), Row(3, 30, "c", null)));
                using var deferredTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                while (new[] { pair.Hosted, pair.Plain }.Any(x => x.Tables["t"].Sent != x.Tables["t"].Enqueued))
                {
                    await pair.Hosted.TriggerAsync();
                    await pair.Plain.TriggerAsync();
                    await Task.Delay(25, deferredTimeout.Token);
                }
                await deferredWatermarks.Task.WaitAsync(deferredTimeout.Token);
                int hostedSaved = pair.Hosted.Completed;
                int plainSaved = pair.Plain.Completed;

                release.TrySetResult();
                string[] expected = ["1|6|x|a2", "2|21|y|b2", "3|30|null|c"];
                var until = DateTime.UtcNow.AddSeconds(30);
                while (!(pair.Hosted.Result().SequenceEqual(expected) && pair.Plain.Result().SequenceEqual(expected)) && DateTime.UtcNow < until)
                {
                    await Task.Delay(25);
                }

                Assert.Equal(expected, pair.Plain.Result());
                Assert.Equal(expected, pair.Hosted.Result());
                Assert.Equal(hostedSaved, pair.Hosted.Completed);
                Assert.Equal(plainSaved, pair.Plain.Completed);
                Assert.Equal(pair.Plain.Destination!.Operations.Order(StringComparer.Ordinal), pair.Hosted.Destination!.Operations.Order(StringComparer.Ordinal));
            }
            finally
            {
                release.TrySetResult();
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AStateWrittenWithAnotherLayoutIsRefused(bool hostedFirst)
        {
            var destination = new DifferentialDestination() { CanHost = hostedFirst };
            await using var stream = new DifferentialStream(() => Copy(true), null, Table("t", Row(1, 5, "a", "x"))) { Destination = destination };
            await stream.StartAsync();
            var until = DateTime.UtcNow.AddSeconds(30);
            while (stream.Completed == 0 && DateTime.UtcNow < until)
            {
                await Task.Delay(25);
            }
            Assert.Equal(["1|5|x|a"], stream.Result());
            Assert.Equal(hostedFirst ? 0 : 1, stream.Stream.GetDiagnosticsGraph().Nodes.Values.Count(x => x.DisplayName == "Fetch required columns"));

            destination.CanHost = !hostedFirst;
            await stream.RestartAsync(waitForRunning: false);
            until = DateTime.UtcNow.AddSeconds(30);
            while (stream.Failures.IsEmpty && DateTime.UtcNow < until)
            {
                await Task.Delay(25);
            }

            Assert.Contains(stream.Failures, x => x != null && x.ToString().Contains("The output tree cannot be read with another layout."));
        }

        [Fact]
        public async Task ASinkWhoseKeyIsFetchedRefusesToStart()
        {
            var plan = Write(new FetchRequiredColumnsRelation()
            {
                Input = new ReadRelation()
                {
                    NamedTable = new NamedTable() { Names = ["t"] },
                    BaseSchema = new NamedStruct()
                    {
                        Names = ["k", "n", "text", "note"],
                        Struct = new Struct() { Types = [new AnyType(), new AnyType(), new AnyType(), new AnyType()] }
                    },
                    RowStore = new RowStoreDefinition() { Name = "t", StoredColumns = [2, 3] }
                },
                Fetches = [new RowStoreFetch() { ReferenceColumn = 2, StoreName = "t", Columns = [0, 1] }],
                Emit = [3, 0, 1, 4]
            }, 4);
            await using var stream = new DifferentialStream(() => plan, null, Table("t", Row(1, 5, "a", "x"))) { Destination = new DifferentialDestination() };

            await stream.StartAsync();
            var until = DateTime.UtcNow.AddSeconds(30);
            while (stream.Failures.IsEmpty && DateTime.UtcNow < until)
            {
                await Task.Delay(25);
            }

            Assert.Contains(stream.Failures, x => x != null && x.ToString().Contains("has column 0 in its primary key and the plan reads that column through a fetch"));
        }
    }
}
