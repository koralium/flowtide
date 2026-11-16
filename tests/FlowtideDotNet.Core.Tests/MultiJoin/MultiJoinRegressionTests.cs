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
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinRegressionTests : IAsyncLifetime
    {
        private readonly string _directory = $"./data/multiJoinRegressionTests/{Guid.NewGuid():N}";
        private readonly List<IDisposable> _disposables = new List<IDisposable>();
        private readonly List<IAsyncDisposable> _asyncDisposables = new List<IAsyncDisposable>();

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            foreach (var item in _asyncDisposables)
            {
                await item.DisposeAsync();
            }
            foreach (var item in _disposables)
            {
                item.Dispose();
            }
            try
            {
                if (Directory.Exists(_directory))
                {
                    Directory.Delete(_directory, true);
                }
            }
            catch (IOException)
            {
            }
        }

        private static NamedStruct Schema(int columns)
        {
            return new NamedStruct()
            {
                Names = Enumerable.Range(0, columns).Select(x => "c" + x).ToList(),
                Struct = new Struct() { Types = Enumerable.Range(0, columns).Select(_ => (SubstraitBaseType)new AnyType()).ToList() }
            };
        }

        private static VirtualTableReadRelation VirtualTable(int columns)
        {
            return new VirtualTableReadRelation()
            {
                BaseSchema = Schema(columns),
                Values = new VirtualTable() { Expressions = new List<StructExpression>() }
            };
        }

        private static ReadRelation Read(string name, int columns)
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { name } },
                BaseSchema = Schema(columns)
            };
        }

        private static FieldReference Field(int index)
        {
            return new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = index } };
        }

        private static MergeJoinRelation Join(JoinType type, Relation left, Relation right, params (int left, int right)[] keys)
        {
            return new MergeJoinRelation()
            {
                Type = type,
                Left = left,
                Right = right,
                LeftKeys = keys.Select(x => Field(x.left)).ToList(),
                RightKeys = keys.Select(x => Field(x.right)).ToList()
            };
        }

        private static StreamEventBatch Batch((long c0, long c1)[] rows, int[] weights)
        {
            var col0 = Column.Create(GlobalMemoryManager.Instance);
            var col1 = Column.Create(GlobalMemoryManager.Instance);
            var weightList = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            var iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
            for (int i = 0; i < rows.Length; i++)
            {
                col0.Add(new Int64Value(rows[i].c0));
                col1.Add(new Int64Value(rows[i].c1));
                weightList.Add(weights[i]);
                iterations.Add(0);
            }
            return new StreamEventBatch(new EventBatchWeighted(weightList, iterations, new EventBatchData(new IColumn[] { col0, col1 })));
        }

        private static StreamEventBatch Batch3((long c0, long c1, long c2)[] rows, int[] weights)
        {
            var col0 = Column.Create(GlobalMemoryManager.Instance);
            var col1 = Column.Create(GlobalMemoryManager.Instance);
            var col2 = Column.Create(GlobalMemoryManager.Instance);
            var weightList = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            var iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
            for (int i = 0; i < rows.Length; i++)
            {
                col0.Add(new Int64Value(rows[i].c0));
                col1.Add(new Int64Value(rows[i].c1));
                col2.Add(new Int64Value(rows[i].c2));
                weightList.Add(weights[i]);
                iterations.Add(0);
            }
            return new StreamEventBatch(new EventBatchWeighted(weightList, iterations, new EventBatchData(new IColumn[] { col0, col1, col2 })));
        }

        private static Task SendBatch(ITargetBlock<IStreamEvent> target, StreamEventBatch batch, long time)
        {
            batch.Data.Rent(1);
            return target.SendAsync(new StreamMessage<StreamEventBatch>(batch, time));
        }

        private async Task<(MultiJoinOperator op, BufferBlock<IStreamEvent> output)> StartOperator(MultiJoinPlan plan, string folder)
        {
            var functions = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(functions);
            var op = new MultiJoinOperator(plan, functions, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            _asyncDisposables.Add(op);
            var output = new BufferBlock<IStreamEvent>();
            op.LinkTo(output, new DataflowLinkOptions() { PropagateCompletion = true });
            op.Setup("stream", "1");
            op.CreateBlock();
            op.Link();

            var storage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"{_directory}/{folder}/persistent" });
            _disposables.Add(storage);
            var meter = new Meter("statemanager");
            _disposables.Add(meter);
            var stateManager = new StateManagerSync<StreamState>(
                new StateManagerOptions()
                {
                    PersistentStorage = storage,
                    TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"{_directory}/{folder}/tmp" }
                },
                NullLoggerFactory.Instance,
                meter,
                "stream",
                GlobalMemoryManager.Instance);
            _disposables.Add(stateManager);
            await stateManager.InitializeAsync();

            var streamMemory = new StreamMemoryManager("stream");
            _disposables.Add(streamMemory);
            var opMeter = new Meter("op");
            _disposables.Add(opMeter);
            var handler = new FlowtideDotNet.Base.Engine.Internal.VertexHandler(
                "stream",
                "1",
                (_, _) => { },
                (_, _, _) => Task.CompletedTask,
                new FlowtideDotNet.Base.Metrics.StreamMetrics("stream").GetOrCreateVertexMeter("1", () => ""),
                stateManager.GetOrCreateClient("1"),
                NullLoggerFactory.Instance,
                streamMemory.CreateOperatorMemoryManager("1"),
                (_, _) => Task.CompletedTask);
            await op.Initialize("1", 0, 0, handler, null);
            return (op, output);
        }

        private static async Task DrainCheckpoint(MultiJoinOperator op, BufferBlock<IStreamEvent> output, long time)
        {
            foreach (var target in op.Targets)
            {
                await target.SendAsync(new Checkpoint(time, time + 1, 1));
            }
            while (true)
            {
                var receiveTask = output.ReceiveAsync(TimeSpan.FromSeconds(5));
                var finished = await Task.WhenAny(receiveTask, op.Completion);
                if (finished == op.Completion)
                {
                    await op.Completion;
                }
                var ev = await receiveTask;
                if (ev is ICheckpointEvent)
                {
                    return;
                }
                if (ev is StreamMessage<StreamEventBatch> message)
                {
                    message.Data.Data.Return();
                }
            }
        }

        [Fact]
        public void MarkedJoinInsideLoopBodyIsRejectedWhenBuilt()
        {
            var loopRead = new IterationReferenceReadRelation() { IterationName = "loop", ReferenceOutputLength = 2 };
            var inner = Join(JoinType.Inner, loopRead, VirtualTable(2), (0, 2));
            var outer = Join(JoinType.Inner, inner, VirtualTable(2), (0, 4));
            inner.MultiJoinGroup = 1;
            outer.MultiJoinGroup = 1;
            outer.Emit = new List<int>() { 0, 1 };

            var iteration = new IterationRelation()
            {
                IterationName = "loop",
                Input = VirtualTable(2),
                LoopPlan = outer,
                MaxIterations = 3
            };
            var plan = new Plan() { Relations = new List<Relation>() { iteration } };
            var functions = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(functions);
            var visitor = new SubstraitVisitor(
                plan,
                new DataflowStreamBuilder("loop_test"),
                null,
                null,
                100,
                functions,
                1,
                TimeSpan.FromSeconds(1),
                true,
                NullLoggerFactory.Instance);

            var error = Assert.Throws<NotSupportedException>(() => visitor.BuildPlan());
            Assert.Contains("multi join cannot run inside a loop", error.Message);
        }

        [Fact]
        public void MarkedGroupALoopRefersToWithoutReadingTheLoopIsBuilt()
        {
            // The group reads no loop: it runs once outside, the loop only reads its result
            var inner = Join(JoinType.Inner, VirtualTable(2), VirtualTable(2), (0, 2));
            var outer = Join(JoinType.Inner, inner, VirtualTable(2), (0, 4));
            inner.MultiJoinGroup = 1;
            outer.MultiJoinGroup = 1;
            outer.Emit = new List<int>() { 0, 1 };
            var loopRead = new IterationReferenceReadRelation() { IterationName = "loop", ReferenceOutputLength = 2 };
            var loopJoin = Join(JoinType.Inner, loopRead, new ReferenceRelation() { RelationId = 1, ReferenceOutputLength = 2 }, (0, 2));
            loopJoin.Emit = new List<int>() { 0, 1 };
            var iteration = new IterationRelation()
            {
                IterationName = "loop",
                Input = VirtualTable(2),
                LoopPlan = loopJoin,
                MaxIterations = 3
            };
            var plan = new Plan() { Relations = new List<Relation>() { iteration, outer } };
            Assert.Empty(MultiJoinLoopBodies.Find(plan));
            var functions = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(functions);
            var visitor = new SubstraitVisitor(
                plan,
                new DataflowStreamBuilder("loop_reference_test"),
                null,
                null,
                100,
                functions,
                1,
                TimeSpan.FromSeconds(1),
                true,
                NullLoggerFactory.Instance);

            visitor.BuildPlan();
        }

        [Fact]
        public void LoopBodyReferencedByIdIsNotMarkedOrGrouped()
        {
            var loopRead = new IterationReferenceReadRelation() { IterationName = "loop", ReferenceOutputLength = 2 };
            var j1 = Join(JoinType.Inner, loopRead, Read("a", 2), (0, 2));
            var j2 = Join(JoinType.Inner, j1, Read("b", 2), (0, 4));
            j2.Emit = new List<int>() { 0, 1 };

            var iteration = new IterationRelation()
            {
                IterationName = "loop",
                Input = Read("seed", 2),
                LoopPlan = new ReferenceRelation() { RelationId = 1, ReferenceOutputLength = 2 }
            };
            var plan = new Plan() { Relations = new List<Relation>() { iteration, j2 } };

            var report = MultiJoinReport.Create(plan, 5);
            int marked = MultiJoinMarker.Mark(plan, 5);

            Assert.Empty(report.Groups);
            Assert.Equal(0, marked);
            Assert.Null(j2.MultiJoinGroup);
        }

        [Fact]
        public void NonEmittedLookupStepsDoNotCopyLeafColumnsOrMarkTargetKeysNeeded()
        {
            const string tables = @"
                CREATE TABLE fact (id any, k1 any, k2 any);
                CREATE TABLE d1 (k any, v any);
                CREATE TABLE d2 (k any, v any);
            ";
            var optimized = MultiJoinPlanBuilderTests.Optimized(@"
                INSERT INTO output
                SELECT f.id
                FROM fact f
                INNER JOIN d1 a1 ON f.k1 = a1.k
                INNER JOIN d2 a2 ON f.k2 = a2.k;", tables);
            var plan = MultiJoinPlanBuilder.Discover(MultiJoinPlanBuilderTests.RootJoin(optimized), 5)!;
            var program = MultiJoinProgram.ForChange(plan, 0);

            Assert.All(program.CopyColumns, Assert.False);
            Assert.Empty(plan.Inputs[1].NeededColumns);
            Assert.Empty(plan.Inputs[2].NeededColumns);
        }

        [Fact]
        public async Task MarkJoinUnderLeftJoinLeavesUpperPadTableCleanOnRightSideMarkFlip()
        {
            var mark = Join(JoinType.LeftMark, Read("a", 2), Read("b", 3), (0, 2));
            var root = Join(JoinType.Left, Read("d", 2), mark, (0, 2));
            var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
            var (op, output) = await StartOperator(plan, "mark_under_left");

            await SendBatch(op.Targets[0], Batch(new[] { (1L, 10L) }, new[] { 1 }), 1);
            await SendBatch(op.Targets[1], Batch(new[] { (1L, 20L) }, new[] { 1 }), 1);
            await DrainCheckpoint(op, output, 2);

            long keysBefore = op.PadTables[1].KeysAdded;
            await SendBatch(op.Targets[2], Batch3(new[] { (1L, 0L, 0L) }, new[] { 1 }), 3);
            await DrainCheckpoint(op, output, 4);

            Assert.False(op.PadTables[1].HasRows);
            Assert.Equal(keysBefore, op.PadTables[1].KeysAdded);
        }

        [Fact]
        public async Task CursorDisposeReleasesSecondaryLookupBuffersOnUnfinishedRun()
        {
            var root = Join(JoinType.Inner, Join(JoinType.Inner, Read("a", 2), Read("b", 3), (0, 2)), Read("c", 2), (3, 5));
            var plan = MultiJoinPlanBuilder.Discover(root, 5)!;

            var storage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"{_directory}/cursor_leak/persistent" });
            _disposables.Add(storage);
            var meter = new Meter("cursor_leak");
            _disposables.Add(meter);
            var stateManager = new StateManagerSync<StreamState>(
                new StateManagerOptions()
                {
                    PersistentStorage = storage,
                    TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"{_directory}/cursor_leak/tmp" }
                },
                NullLoggerFactory.Instance,
                meter,
                "cursor_leak",
                GlobalMemoryManager.Instance);
            _disposables.Add(stateManager);
            await stateManager.InitializeAsync();
            var client = stateManager.GetOrCreateClient("1");

            var inputs = plan.Inputs.Select((p, idx) => new MultiJoinInputState(p, idx)).ToArray();
            foreach (var input in inputs)
            {
                await input.Initialize(client, GlobalMemoryManager.Instance);
            }

            var aBatch = Batch(new[] { (1L, 10L) }, new[] { 1 });
            var bBatch = Batch3(new[] { (1L, 5L, 100L), (1L, 5L, 101L), (1L, 5L, 102L) }, new[] { 1, 1, 1 });
            var cBatch = Batch(new[] { (5L, 999L) }, new[] { 1 });
            aBatch.Data.Rent(1);
            bBatch.Data.Rent(1);
            cBatch.Data.Rent(1);
            try
            {
                await inputs[0].Insert(aBatch.Data.EventBatchData, aBatch.Data.Weights, 1, GlobalMemoryManager.Instance);
                await inputs[1].Insert(bBatch.Data.EventBatchData, bBatch.Data.Weights, 3, GlobalMemoryManager.Instance);

                using var cursorMemory = new StreamMemoryManager("cursor_only");
                var cursorAllocator = cursorMemory.CreateOperatorMemoryManager("c");
                var cursor = new MultiJoinCursor(plan, MultiJoinProgram.ForChange(plan, 2), inputs, Array.Empty<MultiJoinPadTable>(), cursorAllocator, rowBudget: 1);

                Assert.True(cursor.Start(cBatch.Data));
                Assert.True(await cursor.MoveNext());
                cursor.Current.Data.Return();

                cursor.Dispose();
                Assert.Equal(0, cursorMemory.GetAllocatedMemory());
            }
            finally
            {
                aBatch.Data.Return();
                bBatch.Data.Return();
                cBatch.Data.Return();
            }
        }

        [Fact]
        public void SubsetJoinKeySharesPrimaryTreeRegardlessOfJoinOrder()
        {
            var optimized = MultiJoinPlanBuilderTests.Optimized(@"
                INSERT INTO output
                SELECT a.a, b.v, c.name
                FROM extra a
                INNER JOIN extra b ON a.a = b.a AND a.b = b.b
                INNER JOIN companies c ON b.b = c.companyid;");
            var plan = MultiJoinPlanBuilder.Discover(MultiJoinPlanBuilderTests.RootJoin(optimized), 5)!;

            Assert.Empty(plan.Inputs[1].SecondaryLayouts);
        }

        [Fact]
        public void RemarkingMarkedPlanRespectsSmallerInputCap()
        {
            const string starTables = @"
                CREATE TABLE fact (id any, k1 any, k2 any, k3 any, k4 any);
                CREATE TABLE d1 (k any, v any);
                CREATE TABLE d2 (k any, v any);
                CREATE TABLE d3 (k any, v any);
                CREATE TABLE d4 (k any, v any);
            ";
            var plan = MultiJoinPlanBuilderTests.Optimized(@"
                INSERT INTO output
                SELECT f.id, a1.v, a2.v, a3.v, a4.v
                FROM fact f
                INNER JOIN d1 a1 ON f.k1 = a1.k
                INNER JOIN d2 a2 ON f.k2 = a2.k
                INNER JOIN d3 a3 ON f.k3 = a3.k
                INNER JOIN d4 a4 ON f.k4 = a4.k;", starTables);

            MultiJoinMarker.Mark(plan, 5);
            MultiJoinMarker.Mark(plan, 3);
            var built = MultiJoinPlanBuilder.Build(MultiJoinPlanBuilderTests.RootJoin(plan));

            Assert.Equal(3, built.Inputs.Count);
        }

        [Fact]
        public void AJoinOfGroupZeroHashesApartFromAnUnmarkedOne()
        {
            var left = Read("a", 2);
            var right = Read("b", 2);
            var unmarked = Join(JoinType.Inner, left, right, (0, 2));
            var groupZero = Join(JoinType.Inner, left, right, (0, 2));
            groupZero.MultiJoinGroup = 0;

            Assert.NotEqual(unmarked, groupZero);
            Assert.NotEqual(unmarked.GetHashCode(), groupZero.GetHashCode());
        }

        [Fact]
        public void RightJoinWithKeptSideConditionIsGroupedLikeMirroredLeftJoin()
        {
            var rightKeptCondition = MultiJoinPlanBuilderTests.Optimized(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                RIGHT JOIN companies c ON u.companyid = c.companyid AND c.name = 'acme';");
            var planWithRightCheck = MultiJoinPlanBuilder.Discover(MultiJoinPlanBuilderTests.RootJoin(rightKeptCondition), 5);
            Assert.NotNull(planWithRightCheck);
        }

        [Fact]
        public void ConditionOnOneInputOfAJoinedSideBecomesAFilterOnThatInput()
        {
            // (o join u) right join c, with a condition that reads only o
            var lower = Join(JoinType.Inner, Read("o", 2), Read("u", 2), (0, 2));
            var root = Join(JoinType.Right, lower, Read("c", 2), (3, 4));
            root.PostJoinFilter = new ScalarFunction()
            {
                ExtensionUrn = FunctionsComparison.Urn,
                ExtensionName = FunctionsComparison.GreaterThan,
                Arguments = new List<Expression>()
                {
                    new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = 1 } },
                    new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = 0 } }
                }
            };

            var plan = MultiJoinPlanBuilder.Discover(root, 5);

            Assert.NotNull(plan);
            Assert.Equal(3, plan.Inputs.Count);
            var filtered = Assert.IsType<FilterRelation>(plan.Inputs[0].Relation);
            Assert.IsType<ReadRelation>(filtered.Input);
            Assert.IsType<ReadRelation>(plan.Inputs[1].Relation);
            Assert.Empty(plan.Root.Checks);
            Assert.Null(plan.Root.KeptOnlyCheck);
        }
    }
}
