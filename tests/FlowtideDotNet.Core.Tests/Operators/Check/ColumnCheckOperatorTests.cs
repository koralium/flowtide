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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Engine.Internal;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.Check;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Operators.Check
{
    public class ColumnCheckOperatorTests : OperatorTestBase, IDisposable
    {
        private const string CheckId = "1:0";

        private sealed class RecordingPublisher : ICheckIssuePublisher
        {
            public List<CheckIssueBatch> Batches { get; } = new List<CheckIssueBatch>();

            public List<string> Discarded { get; } = new List<string>();

            public bool IssuesEnabled { get; set; } = true;

            public bool StatusEnabled { get; set; }

            public void Enqueue(CheckIssueBatch batch)
            {
                lock (Batches)
                {
                    Batches.Add(batch);
                }
            }

            public void Publish(CheckIssueBatch batch)
            {
                Enqueue(batch);
            }

            public void DiscardPending(string checkId)
            {
                lock (Discarded)
                {
                    Discarded.Add(checkId);
                }
            }

            public List<CheckIssueBatch> Take()
            {
                lock (Batches)
                {
                    var batches = Batches.ToList();
                    Batches.Clear();
                    return batches;
                }
            }
        }

        // The default storage directory is shared with parallel test classes
        private readonly string _directory = $"./data/checkOperatorTests/{Guid.NewGuid():N}";
        private FileCachePersistentStorage? _storage;

        protected override StateManagerOptions CreateStateManagerOptions()
        {
            _storage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"{_directory}/persistent" });
            return new StateManagerOptions()
            {
                PersistentStorage = _storage,
                TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"{_directory}/tmp" }
            };
        }

        void IDisposable.Dispose()
        {
            Dispose();
            _storage?.Dispose();
        }

        private readonly RecordingPublisher _publisher = new RecordingPublisher();
        private readonly BufferBlock<IStreamEvent> _output = new BufferBlock<IStreamEvent>();
        private long _checkpointTime;

        private const string CheckName = "Check {message} {tag}";

        // Columns are message tag, condition, tag and guard
        private record Row(string? Message, bool? Ok, long Tag, int Weight, IDataValue? Guard = null);

        private static DirectFieldReference Field(int index)
        {
            return new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = index } };
        }

        // Fails on ok = false, one issue for the whole check
        private static CheckDefinition TaglessCheck(string name)
        {
            return new CheckDefinition()
            {
                Condition = Field(1),
                Message = name,
                Tags = new List<CheckTag>(),
                Guards = new List<CheckGuard>()
            };
        }

        private static CheckRelation CreateRelation(List<CheckGuard>? guards = null, List<int>? emit = null, List<CheckDefinition>? checks = null)
        {
            return new CheckRelation()
            {
                Input = new ReadRelation()
                {
                    NamedTable = new NamedTable() { Names = new List<string>() { "t" } },
                    BaseSchema = new NamedStruct()
                    {
                        Names = new List<string>() { "message", "ok", "tag", "guard" },
                        Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType(), new AnyType(), new AnyType(), new AnyType() } }
                    }
                },
                Checks = checks ?? new List<CheckDefinition>()
                {
                    new CheckDefinition()
                    {
                        Condition = Field(1),
                        Message = CheckName,
                        Tags = new List<CheckTag>()
                        {
                            new CheckTag() { Key = "message", Value = Field(0) },
                            new CheckTag() { Key = "tag", Value = Field(2) }
                        },
                        Guards = guards ?? new List<CheckGuard>()
                    }
                },
                Emit = emit
            };
        }

        private List<CheckIssueBatch> _startItems = new List<CheckIssueBatch>();

        private async Task<ColumnCheckOperator> StartOperator(CheckRelation? relation = null, string[]? checkIds = null)
        {
            checkIds ??= new[] { CheckId };
            var op = new ColumnCheckOperator(relation ?? CreateRelation(), FunctionsRegister, _publisher, checkIds, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = 100,
                MaxDegreeOfParallelism = 1
            });
            op.LinkTo(_output);
            await InitializeOperator(op);
            _startItems = _publisher.Take();
            // One empty start item per check, in check order, when anyone listens
            Assert.Equal(_publisher.IssuesEnabled || _publisher.StatusEnabled ? checkIds.Length : 0, _startItems.Count);
            Assert.Equal(checkIds.Take(_startItems.Count), _startItems.Select(x => x.CheckId));
            Assert.All(_startItems, x => Assert.Empty(x.Changes));
            Assert.All(_startItems, x => Assert.Equal(_publisher.IssuesEnabled, x.IsSnapshot));
            Assert.All(_startItems, x => Assert.Equal(_publisher.StatusEnabled, x.Status.HasValue));
            return op;
        }

        private static StreamEventBatch CreateBatch(params Row[] rows)
        {
            var columns = new IColumn[4];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
            }
            var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            var iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
            foreach (var row in rows)
            {
                if (row.Message == null)
                {
                    columns[0].Add(NullValue.Instance);
                }
                else
                {
                    columns[0].Add(new StringValue(row.Message));
                }
                if (row.Ok == null)
                {
                    columns[1].Add(NullValue.Instance);
                }
                else
                {
                    columns[1].Add(row.Ok.Value ? BoolValue.True : BoolValue.False);
                }
                columns[2].Add(new Int64Value(row.Tag));
                columns[3].Add(row.Guard ?? NullValue.Instance);
                weights.Add(row.Weight);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            // The upstream rent for the operator link
            batch.Rent(1);
            return batch;
        }

        private async Task<StreamEventBatch> Send(ColumnCheckOperator op, params Row[] rows)
        {
            var batch = CreateBatch(rows);
            await op.SendAsync(new StreamMessage<StreamEventBatch>(batch, 0));
            var output = await Receive(op);
            var message = Assert.IsType<StreamMessage<StreamEventBatch>>(output);
            return message.Data;
        }

        private async Task<List<CheckIssueBatch>> Checkpoint(ColumnCheckOperator op, long version)
        {
            await op.SendAsync(new Checkpoint(_checkpointTime, _checkpointTime + 1, version));
            _checkpointTime++;
            var output = await Receive(op);
            Assert.IsType<Checkpoint>(output);
            return _publisher.Take();
        }

        // Surfaces an operator fault instead of a receive timeout
        private async Task<IStreamEvent> Receive(ColumnCheckOperator op)
        {
            var receive = _output.ReceiveAsync(TimeSpan.FromSeconds(10));
            await Task.WhenAny(receive, op.Completion);
            if (op.Completion.IsFaulted)
            {
                throw new InvalidOperationException("The operator faulted.", op.Completion.Exception);
            }
            return await receive;
        }

        // The message tag first, then the other tags
        private static List<string> Describe(CheckIssueBatch batch)
        {
            Assert.Equal(CheckName, batch.CheckName);
            return batch.Changes
                .Select(x => $"{(x.Active ? "+" : "-")}{x.Tags[0].Value ?? "null"}:{string.Join(",", x.Tags.Skip(1).Select(t => $"{t.Key}={t.Value ?? "null"}"))}")
                .ToList();
        }

        [Fact]
        public async Task CheckpointPublishesRaisedIssues()
        {
            var op = await StartOperator();
            Assert.Equal(new[] { CheckId }, _publisher.Discarded);

            (await Send(op,
                new Row("a", false, 1, 1),
                new Row("b", true, 2, 1),
                new Row("c", null, 3, 1),
                new Row(null, false, 4, 1))).Return();

            var batches = await Checkpoint(op, 7);
            var batch = Assert.Single(batches);
            Assert.False(batch.IsSnapshot);
            Assert.Equal(CheckId, batch.CheckId);
            Assert.Equal(7, batch.Version);
            Assert.Equal(new[] { "+null:tag=4", "+a:tag=1" }, Describe(batch));
        }

        [Fact]
        public async Task DisabledPublisherTracksNoTransitions()
        {
            _publisher.IssuesEnabled = false;
            var op = await StartOperator();
            (await Send(op, new Row("a", false, 1, 1))).Return();
            Assert.Empty(await Checkpoint(op, 1));
            (await Send(op, new Row("a", false, 1, -1))).Return();
            Assert.Empty(await Checkpoint(op, 2));
        }

        [Fact]
        public async Task SameMessageAndTagsShareOneIssue()
        {
            var op = await StartOperator();
            await Checkpoint(op, 1);

            (await Send(op, new Row("dup", false, 1, 1), new Row("dup", false, 1, 1))).Return();
            (await Send(op, new Row("dup", false, 1, 1), new Row("dup", false, 2, 1))).Return();
            var batch = Assert.Single(await Checkpoint(op, 2));
            Assert.Equal(new[] { "+dup:tag=1", "+dup:tag=2" }, Describe(batch));

            (await Send(op, new Row("dup", false, 1, -1), new Row("dup", false, 1, -1))).Return();
            Assert.Empty(await Checkpoint(op, 3));

            (await Send(op, new Row("dup", false, 1, -1))).Return();
            batch = Assert.Single(await Checkpoint(op, 4));
            Assert.Equal(new[] { "-dup:tag=1" }, Describe(batch));
        }

        [Fact]
        public async Task RaiseAndResolveInOneEpochPublishesNothing()
        {
            var op = await StartOperator();
            Assert.Empty(await Checkpoint(op, 1));

            (await Send(op, new Row("a", false, 1, 1))).Return();
            (await Send(op, new Row("a", false, 1, -1))).Return();
            Assert.Empty(await Checkpoint(op, 2));

            (await Send(op, new Row("b", false, 1, 1), new Row("b", false, 1, -1))).Return();
            Assert.Empty(await Checkpoint(op, 3));
        }

        [Fact]
        public async Task NegativeWeightIsStoredButNeverActive()
        {
            var op = await StartOperator();
            await Checkpoint(op, 1);

            (await Send(op, new Row("a", false, 1, -1))).Return();
            Assert.Empty(await Checkpoint(op, 2));

            (await Send(op, new Row("a", false, 1, 1))).Return();
            Assert.Empty(await Checkpoint(op, 3));

            (await Send(op, new Row("a", false, 1, 1))).Return();
            var batch = Assert.Single(await Checkpoint(op, 4));
            Assert.Equal(new[] { "+a:tag=1" }, Describe(batch));
        }

        [Fact]
        public async Task InputIsPassedThrough()
        {
            var op = await StartOperator();

            var passing = CreateBatch(new Row("a", true, 1, 1));
            await op.SendAsync(new StreamMessage<StreamEventBatch>(passing, 0));
            var output = Assert.IsType<StreamMessage<StreamEventBatch>>(await Receive(op));
            Assert.Same(passing, output.Data);
            output.Data.Return();

            var failing = CreateBatch(new Row("a", false, 1, 1), new Row("b", true, 2, 1));
            await op.SendAsync(new StreamMessage<StreamEventBatch>(failing, 0));
            output = Assert.IsType<StreamMessage<StreamEventBatch>>(await Receive(op));
            Assert.Same(failing, output.Data);
            output.Data.Return();
        }

        [Fact]
        public async Task EmitReusesInputColumns()
        {
            var op = await StartOperator(CreateRelation(emit: new List<int>() { 2, 0 }));

            var input = CreateBatch(new Row("a", false, 1, 1));
            await op.SendAsync(new StreamMessage<StreamEventBatch>(input, 0));
            var output = Assert.IsType<StreamMessage<StreamEventBatch>>(await Receive(op));
            Assert.Equal(2, output.Data.Data.EventBatchData.Columns.Count);
            Assert.Same(input.Data.EventBatchData.Columns[2], output.Data.Data.EventBatchData.Columns[0]);
            Assert.Same(input.Data.EventBatchData.Columns[0], output.Data.Data.EventBatchData.Columns[1]);
            Assert.Same(input.Data.Weights, output.Data.Data.Weights);
            output.Data.Return();

            var batch = Assert.Single(await Checkpoint(op, 1));
            Assert.Equal(new[] { "+a:tag=1" }, Describe(batch));
        }

        // The row message names its guard value
        private static IDataValue GuardValue(string name) => name switch
        {
            "bool-true" => BoolValue.True,
            "bool-false" => BoolValue.False,
            "int-positive" => new Int64Value(5),
            "int-zero" => new Int64Value(0),
            "null" => NullValue.Instance,
            "string" => new StringValue("true"),
            _ => throw new ArgumentException(name)
        };

        [Theory]
        [InlineData(CheckGuardKind.IsTrue, new[] { "bool-true", "bool-false", "int-positive", "int-zero", "null", "string" }, new[] { "+bool-true:tag=1", "+int-positive:tag=3" })]
        [InlineData(CheckGuardKind.IsNotTrue, new[] { "bool-true", "int-zero", "null" }, new[] { "+int-zero:tag=2", "+null:tag=3" })]
        [InlineData(CheckGuardKind.IsNull, new[] { "bool-false", "null" }, new[] { "+null:tag=2" })]
        public async Task GuardsFollowToBoolAndNullSemantics(CheckGuardKind kind, string[] values, string[] expected)
        {
            var op = await StartOperator(CreateRelation(guards: new List<CheckGuard>()
            {
                new CheckGuard() { Expression = Field(3), Kind = kind }
            }));
            (await Send(op, values.Select((x, i) => new Row(x, false, i + 1, 1, GuardValue(x))).ToArray())).Return();
            var batch = Assert.Single(await Checkpoint(op, 1));
            Assert.Equal(expected, Describe(batch));
        }

        [Fact]
        public async Task ReinitializeDiscardsAndPublishesSnapshot()
        {
            var op = await StartOperator();
            (await Send(op, new Row("a", false, 1, 1), new Row("b", false, 2, 1))).Return();
            await Checkpoint(op, 1);
            (await Send(op, new Row("b", false, 2, -1), new Row("c", false, 3, 1))).Return();
            Assert.Single(await Checkpoint(op, 2));

            // Transitions after the last seal are superseded by the snapshot
            (await Send(op, new Row("d", false, 4, 1))).Return();
            await ReinitializeOperator(op);
            Assert.Equal(new[] { CheckId, CheckId }, _publisher.Discarded);

            // The harness does not roll back, so d is still in the tree
            var batch = Assert.Single(_publisher.Take());
            Assert.True(batch.IsSnapshot);
            Assert.Equal(new[] { "+a:tag=1", "+c:tag=3", "+d:tag=4" }, Describe(batch));
            Assert.Empty(await Checkpoint(op, 3));

            (await Send(op, new Row("a", false, 1, -1))).Return();
            batch = Assert.Single(await Checkpoint(op, 4));
            Assert.False(batch.IsSnapshot);
            Assert.Equal(new[] { "-a:tag=1" }, Describe(batch));
        }

        [Fact]
        public async Task CountsMatchTreeScanAfterRandomChanges()
        {
            _publisher.StatusEnabled = true;
            var random = new Random(20261004);
            var messages = new string?[] { "a", "b", "c", null };
            var weights = new[] { -2, -1, 1, 1, 2, 3 };
            var model = new Dictionary<(string?, long), long>();
            var op = await StartOperator();
            var lastStatus = new CheckStatus(0, 0);

            for (int round = 0; round < 60; round++)
            {
                var rows = new Row[random.Next(1, 25)];
                for (int i = 0; i < rows.Length; i++)
                {
                    bool? ok = random.Next(4) switch { 0 => true, 1 => null, _ => false };
                    rows[i] = new Row(messages[random.Next(messages.Length)], ok, random.Next(4), weights[random.Next(weights.Length)]);
                    if (ok == false)
                    {
                        var key = (rows[i].Message, rows[i].Tag);
                        model[key] = model.GetValueOrDefault(key) + rows[i].Weight;
                    }
                }
                (await Send(op, rows)).Return();

                var expected = new CheckStatus(model.Values.Count(x => x > 0), model.Values.Where(x => x > 0).Sum());
                Assert.Equal(expected, op.GetCountsForTests(0));
                Assert.Equal(expected, await op.ScanCountsForTests(0));

                if (round % 7 == 6)
                {
                    var status = (await Checkpoint(op, round)).SingleOrDefault()?.Status;
                    Assert.Equal(expected != lastStatus ? expected : (CheckStatus?)null, status);
                    lastStatus = expected;
                }
            }
            Assert.Contains(model.Values, x => x < 0);
            Assert.Contains(model.Values, x => x > 1);

            await Checkpoint(op, 100);
            await ReinitializeOperator(op);
            var restored = Assert.Single(_publisher.Take());
            var scanned = await op.ScanCountsForTests(0);
            Assert.Equal(scanned, op.GetCountsForTests(0));
            Assert.Equal(scanned, restored.Status);
        }

        [Fact]
        public async Task StatusAtStartComesWithTheSnapshot()
        {
            _publisher.StatusEnabled = true;
            var op = await StartOperator();
            var start = Assert.Single(_startItems);
            Assert.True(start.IsSnapshot);
            Assert.Empty(start.Changes);
            Assert.Equal(new CheckStatus(0, 0), start.Status);

            (await Send(op, new Row("a", false, 1, 1), new Row("a", false, 1, 1), new Row("b", false, 2, 1))).Return();
            var batch = Assert.Single(await Checkpoint(op, 1));
            Assert.False(batch.IsSnapshot);
            Assert.Equal(1, batch.Version);
            Assert.Equal(new[] { "+a:tag=1", "+b:tag=2" }, Describe(batch));
            Assert.Equal(new CheckStatus(2, 3), batch.Status);

            await ReinitializeOperator(op);
            var restored = Assert.Single(_publisher.Take());
            Assert.True(restored.IsSnapshot);
            Assert.Equal(new[] { "+a:tag=1", "+b:tag=2" }, Describe(restored));
            Assert.Equal(new CheckStatus(2, 3), restored.Status);
            Assert.Empty(await Checkpoint(op, 2));
        }

        [Fact]
        public async Task StatusIsOnlyReportedWhenCountsChange()
        {
            _publisher.StatusEnabled = true;
            var op = await StartOperator();
            Assert.Empty(await Checkpoint(op, 1));

            (await Send(op, new Row("a", false, 1, 1))).Return();
            var batch = Assert.Single(await Checkpoint(op, 2));
            Assert.Equal(new[] { "+a:tag=1" }, Describe(batch));
            Assert.Equal(new CheckStatus(1, 1), batch.Status);

            Assert.Empty(await Checkpoint(op, 3));

            // Issues change but the counts do not
            (await Send(op, new Row("a", false, 1, -1), new Row("b", false, 2, 1))).Return();
            batch = Assert.Single(await Checkpoint(op, 4));
            Assert.Equal(new[] { "-a:tag=1", "+b:tag=2" }, Describe(batch));
            Assert.Null(batch.Status);

            // Counts change but the issues do not
            (await Send(op, new Row("b", false, 2, 1))).Return();
            batch = Assert.Single(await Checkpoint(op, 5));
            Assert.Empty(batch.Changes);
            Assert.Equal(new CheckStatus(1, 2), batch.Status);

            // Changes that cancel out within the epoch
            (await Send(op, new Row("b", false, 2, -1))).Return();
            (await Send(op, new Row("b", false, 2, 1))).Return();
            Assert.Empty(await Checkpoint(op, 6));

            (await Send(op, new Row("b", false, 2, -2))).Return();
            batch = Assert.Single(await Checkpoint(op, 7));
            Assert.Equal(new[] { "-b:tag=2" }, Describe(batch));
            Assert.Equal(new CheckStatus(0, 0), batch.Status);
        }

        [Fact]
        public async Task TaglessCheckCountsEveryFailingRowInOneIssue()
        {
            _publisher.StatusEnabled = true;
            var op = await StartOperator(CreateRelation(checks: new List<CheckDefinition>() { TaglessCheck("Ok is false") }));
            Assert.Equal(new CheckStatus(0, 0), Assert.Single(_startItems).Status);

            (await Send(op,
                new Row("a", false, 1, 1),
                new Row("b", false, 2, 1),
                new Row("c", false, 3, 2),
                new Row("d", true, 4, 1),
                new Row("e", false, 5, 1))).Return();
            var batch = Assert.Single(await Checkpoint(op, 1));
            Assert.Equal("Ok is false", batch.CheckName);
            var change = Assert.Single(batch.Changes);
            Assert.True(change.Active);
            Assert.Empty(change.Tags);
            Assert.Equal(new CheckStatus(1, 5), batch.Status);

            (await Send(op, new Row("a", false, 1, -1), new Row("c", false, 3, -1))).Return();
            batch = Assert.Single(await Checkpoint(op, 2));
            Assert.Empty(batch.Changes);
            Assert.Equal(new CheckStatus(1, 3), batch.Status);
            Assert.Equal(new CheckStatus(1, 3), await op.ScanCountsForTests(0));

            (await Send(op, new Row("x", false, 9, -3))).Return();
            batch = Assert.Single(await Checkpoint(op, 3));
            change = Assert.Single(batch.Changes);
            Assert.False(change.Active);
            Assert.Empty(change.Tags);
            Assert.Equal(new CheckStatus(0, 0), batch.Status);
        }

        [Fact]
        public async Task StatusWithoutIssueListener()
        {
            _publisher.IssuesEnabled = false;
            _publisher.StatusEnabled = true;
            var op = await StartOperator();
            var start = Assert.Single(_startItems);
            Assert.Empty(start.Changes);
            Assert.Equal(new CheckStatus(0, 0), start.Status);

            (await Send(op, new Row("a", false, 1, 1), new Row("a", false, 1, 1))).Return();
            var batch = Assert.Single(await Checkpoint(op, 1));
            Assert.False(batch.IsSnapshot);
            Assert.Empty(batch.Changes);
            Assert.Equal(new CheckStatus(1, 2), batch.Status);

            (await Send(op, new Row("a", false, 1, -2))).Return();
            batch = Assert.Single(await Checkpoint(op, 2));
            Assert.Empty(batch.Changes);
            Assert.Equal(new CheckStatus(0, 0), batch.Status);

            (await Send(op, new Row("b", false, 2, 1))).Return();
            await Checkpoint(op, 3);
            await ReinitializeOperator(op);
            var restored = Assert.Single(_publisher.Take());
            Assert.False(restored.IsSnapshot);
            Assert.Empty(restored.Changes);
            Assert.Equal(new CheckStatus(1, 1), restored.Status);
        }

        [Fact]
        public async Task IssuesWithoutStatusListener()
        {
            var op = await StartOperator();
            Assert.Null(Assert.Single(_startItems).Status);

            (await Send(op, new Row("a", false, 1, 1))).Return();
            var batch = Assert.Single(await Checkpoint(op, 1));
            Assert.Equal(new[] { "+a:tag=1" }, Describe(batch));
            Assert.Null(batch.Status);

            // Only the counts change
            (await Send(op, new Row("a", false, 1, 1))).Return();
            Assert.Empty(await Checkpoint(op, 2));
            Assert.Equal(new CheckStatus(1, 2), op.GetCountsForTests(0));

            await ReinitializeOperator(op);
            var restored = Assert.Single(_publisher.Take());
            Assert.True(restored.IsSnapshot);
            Assert.Null(restored.Status);
        }

        [Fact]
        public async Task GaugesReportEveryCheckWithItsNameAndId()
        {
            // Unique ids, other tests publish the same instruments
            var taggedId = $"{Guid.NewGuid():N}:0";
            var taglessId = $"{Guid.NewGuid():N}:1";
            var observed = new Dictionary<(string Instrument, string CheckId), (long Value, object? CheckName, object? Operator)>();
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Name == "flowtide_check_active_issues" || instrument.Name == "flowtide_check_failing_rows")
                {
                    meterListener.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) =>
            {
                string? checkId = null;
                object? checkName = null;
                object? operatorName = null;
                foreach (var tag in tags)
                {
                    switch (tag.Key)
                    {
                        case "check_id":
                            checkId = tag.Value as string;
                            break;
                        case "check_name":
                            checkName = tag.Value;
                            break;
                        case "operator":
                            operatorName = tag.Value;
                            break;
                    }
                }
                if (checkId == taggedId || checkId == taglessId)
                {
                    observed[(instrument.Name, checkId)] = (value, checkName, operatorName);
                }
            });
            listener.Start();

            var relation = CreateRelation(checks: new List<CheckDefinition>()
            {
                CreateRelation().Checks[0],
                TaglessCheck("Ok is false")
            });
            var op = await StartOperator(relation, new[] { taggedId, taglessId });

            (await Send(op, new Row("a", false, 1, 1), new Row("a", false, 1, 1), new Row("b", false, 2, 1))).Return();

            // Live counts, before any checkpoint
            observed.Clear();
            listener.RecordObservableInstruments();
            Assert.Equal(4, observed.Count);
            Assert.Equal((2L, (object?)CheckName, (object?)"1"), observed[("flowtide_check_active_issues", taggedId)]);
            Assert.Equal((3L, (object?)CheckName, (object?)"1"), observed[("flowtide_check_failing_rows", taggedId)]);
            Assert.Equal((1L, (object?)"Ok is false", (object?)"1"), observed[("flowtide_check_active_issues", taglessId)]);
            Assert.Equal((3L, (object?)"Ok is false", (object?)"1"), observed[("flowtide_check_failing_rows", taglessId)]);

            (await Send(op, new Row("a", false, 1, -2))).Return();
            observed.Clear();
            listener.RecordObservableInstruments();
            Assert.Equal(1, observed[("flowtide_check_active_issues", taggedId)].Value);
            Assert.Equal(1, observed[("flowtide_check_failing_rows", taggedId)].Value);
            Assert.Equal(1, observed[("flowtide_check_active_issues", taglessId)].Value);
            Assert.Equal(1, observed[("flowtide_check_failing_rows", taglessId)].Value);
        }
    }
}
