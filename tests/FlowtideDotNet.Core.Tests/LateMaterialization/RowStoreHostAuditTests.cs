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
using FlowtideDotNet.Base.Engine.Internal;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Core.Operators.Normalization;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Core.Sources.Generic;
using FlowtideDotNet.Core.Sources.Generic.Internal;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Core.Tests.LateMaterialization.RowHeapTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    [Collection(nameof(RowStoreHostRaceCollection))]
    public partial class RowStoreHostAuditTests
    {
        private sealed record Row(long Key, long Kept, string Text, long Number, int Weight = 1);

        private sealed class GenericItem
        {
            public long Id { get; set; }

            public string Text { get; set; } = "";
        }

        private sealed class GatedProvider : IReservoirStorageProvider
        {
            private readonly MemoryFileProvider _inner = new MemoryFileProvider();

            public Func<Task>? OnReadAsync { get; set; }

            public bool SupportsFileListing => _inner.SupportsFileListing;

            public Task DeleteCheckpointFileAsync(CheckpointId checkpointVersion, CancellationToken cancellationToken = default) =>
                _inner.DeleteCheckpointFileAsync(checkpointVersion, cancellationToken);

            public Task DeleteDataFileAsync(ulong fileId, CancellationToken cancellationToken = default) =>
                _inner.DeleteDataFileAsync(fileId, cancellationToken);

            public bool DataFileExists(ulong fileId, CancellationToken cancellationToken = default) =>
                _inner.DataFileExists(fileId, cancellationToken);

            public async ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(ulong fileId, int offset, int length, uint crc32, CancellationToken cancellationToken = default)
            {
                if (OnReadAsync != null)
                {
                    await OnReadAsync();
                }
                return await _inner.GetMemoryAsync(fileId, offset, length, crc32, cancellationToken);
            }

            public async ValueTask<T> ReadAsync<T>(ulong fileId, int offset, int length, uint crc32, FlowtideDotNet.Storage.StateManager.Internal.IStateSerializer<T> stateSerializer, CancellationToken cancellationToken = default)
                where T : FlowtideDotNet.Storage.StateManager.Internal.ICacheObject
            {
                if (OnReadAsync != null)
                {
                    await OnReadAsync();
                }
                return await _inner.ReadAsync(fileId, offset, length, crc32, stateSerializer, cancellationToken);
            }

            public Task<PipeReader> ReadCheckpointFileAsync(CheckpointId checkpointVersion, CancellationToken cancellationToken = default) =>
                _inner.ReadCheckpointFileAsync(checkpointVersion, cancellationToken);

            public Task<PipeReader?> ReadCheckpointRegistryFileAsync(CancellationToken cancellationToken = default) =>
                _inner.ReadCheckpointRegistryFileAsync(cancellationToken);

            public async Task<PipeReader> ReadDataFileAsync(ulong fileId, int fileSize, CancellationToken cancellationToken = default)
            {
                if (OnReadAsync != null)
                {
                    await OnReadAsync();
                }
                return await _inner.ReadDataFileAsync(fileId, fileSize, cancellationToken);
            }

            public Task WriteCheckpointFileAsync(CheckpointId checkpointVersion, PipeReader data, CancellationToken cancellationToken = default) =>
                _inner.WriteCheckpointFileAsync(checkpointVersion, data, cancellationToken);

            public Task WriteCheckpointRegistryFile(PipeReader data, CancellationToken cancellationToken = default) =>
                _inner.WriteCheckpointRegistryFile(data, cancellationToken);

            public Task WriteDataFileAsync(ulong fileId, ulong crc64, int size, bool isBundle, PipeReader data, CancellationToken cancellationToken = default) =>
                _inner.WriteDataFileAsync(fileId, crc64, size, isBundle, data, cancellationToken);

            public Task<IEnumerable<ulong>> GetStoredDataFileIdsAsync(CancellationToken cancellationToken = default) =>
                _inner.GetStoredDataFileIdsAsync(cancellationToken);

            public Task<IEnumerable<ulong>> ListDataFilesAboveVersionAsync(ulong minVersion, CancellationToken cancellationToken = default) =>
                _inner.ListDataFilesAboveVersionAsync(minVersion, cancellationToken);

            public Task InitializeAsync(StorageProviderContext providerContext, CancellationToken cancellationToken = default) =>
                _inner.InitializeAsync(providerContext, cancellationToken);

            public Task<PipeReader?> ReadStreamsMetadataFileAsync(string streamName, CancellationToken cancellationToken = default) =>
                _inner.ReadStreamsMetadataFileAsync(streamName, cancellationToken);

            public Task WriteStreamsMetadataFileAsync(string streamName, PipeReader data, CancellationToken cancellationToken = default) =>
                _inner.WriteStreamsMetadataFileAsync(streamName, data, cancellationToken);

            public Task DeleteStreamVersionAsync(string streamName, string streamVersion, CancellationToken cancellationToken = default) =>
                _inner.DeleteStreamVersionAsync(streamName, streamVersion, cancellationToken);

            public Task<IEnumerable<CheckpointId>> ListCheckpointFilesAsync(CancellationToken cancellationToken = default) =>
                _inner.ListCheckpointFilesAsync(cancellationToken);

            public void Dispose()
            {
            }
        }

        private sealed class ThrowingKeptColumn : Column, IColumn
        {
            public ThrowingKeptColumn() : base(GlobalMemoryManager.Instance)
            {
            }

            IDataValue IColumn.GetValueAt(in int index, in ReferenceSegment? child)
            {
                throw new InvalidOperationException("kept read failed");
            }

            void IColumn.GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
            {
                throw new InvalidOperationException("kept read failed");
            }
        }

        private sealed class GatedGenericDataSource : GenericDataSourceAsync<GenericItem>
        {
            private readonly List<FlowtideGenericObject<GenericItem>> _full;
            private readonly List<FlowtideGenericObject<GenericItem>> _delta;

            public GatedGenericDataSource(List<FlowtideGenericObject<GenericItem>> full, List<FlowtideGenericObject<GenericItem>> delta)
            {
                _full = full;
                _delta = delta;
            }

            public SemaphoreSlim FullEntered { get; } = new SemaphoreSlim(0);

            public SemaphoreSlim DeltaEntered { get; } = new SemaphoreSlim(0);

            public override TimeSpan? DeltaLoadInterval => null;

            public override async IAsyncEnumerable<FlowtideGenericObject<GenericItem>> FullLoadAsync()
            {
                FullEntered.Release();
                await Task.Yield();
                foreach (var item in _full)
                {
                    yield return item;
                }
            }

            public override async IAsyncEnumerable<FlowtideGenericObject<GenericItem>> DeltaLoadAsync(long lastWatermark)
            {
                DeltaEntered.Release();
                await Task.Yield();
                foreach (var item in _delta)
                {
                    yield return item;
                }
            }
        }

        private sealed class InspectableGenericReadOperator : GenericReadOperator<GenericItem>
        {
            public InspectableGenericReadOperator(GenericDataSourceAsync<GenericItem> source, ReadRelation relation)
                : base(source, relation, new FunctionsRegister(), new DataflowBlockOptions())
            {
            }

            public ValueTask<GenericItem?> LookupPublic(string key) => LookupStoredObject(key);
        }

        private sealed class CountingSource : ColumnBatchReadBaseOperator
        {
            private readonly IMemoryAllocator _keyAllocator;
            private readonly IMemoryAllocator _restAllocator;
            private readonly List<int> _keys;
            private readonly bool _appendOnly;
            private long _watermark;

            public CountingSource(
                ReadRelation relation,
                List<(string Key, long Kept, string Text)> rows,
                IMemoryAllocator keyAllocator,
                IMemoryAllocator restAllocator,
                List<int> keys,
                bool appendOnly = false,
                int boundedCapacity = DataflowBlockOptions.Unbounded,
                bool splitBatches = false)
                : base(relation, new FunctionsRegister(), new DataflowBlockOptions() { BoundedCapacity = boundedCapacity })
            {
                Rows = rows;
                _keyAllocator = keyAllocator;
                _restAllocator = restAllocator;
                _keys = keys;
                _appendOnly = appendOnly;
                _splitBatches = splitBatches;
            }

            private readonly bool _splitBatches;

            public List<(string Key, long Kept, string Text)> Rows { get; }

            public SemaphoreSlim LoadEntered { get; } = new SemaphoreSlim(0);

            protected override bool AppendOnly => _appendOnly;

            public override string DisplayName => "counting source";

            public override Task Compact() => Task.CompletedTask;

            protected override Task Checkpoint(long checkpointTime) => Task.CompletedTask;

            protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { "t" });

            protected override ValueTask<List<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(_keys);

            protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
            {
                LoadEntered.Release();
                await Task.Yield();
                if (_splitBatches)
                {
                    foreach (var row in Rows)
                    {
                        var singleColumns = new IColumn[]
                        {
                            Column.Create(_keyAllocator),
                            Column.Create(_restAllocator),
                            Column.Create(_restAllocator)
                        };
                        var singleWeights = new PrimitiveList<int>(_restAllocator) { 1 };
                        var singleIterations = new PrimitiveList<uint>(_restAllocator) { 0 };
                        singleColumns[0].Add(new StringValue(row.Key));
                        singleColumns[1].Add(new Int64Value(row.Kept));
                        singleColumns[2].Add(new StringValue(row.Text));
                        yield return new ColumnReadEvent(new EventBatchWeighted(singleWeights, singleIterations, new EventBatchData(singleColumns)), ++_watermark);
                    }
                    yield break;
                }
                var columns = new IColumn[]
                {
                    Column.Create(_keyAllocator),
                    Column.Create(_restAllocator),
                    Column.Create(_restAllocator),
                    Column.Create(_restAllocator)
                };
                var weights = new PrimitiveList<int>(_restAllocator);
                var iterations = new PrimitiveList<uint>(_restAllocator);
                foreach (var row in Rows)
                {
                    columns[0].Add(new StringValue(row.Key));
                    columns[1].Add(new Int64Value(row.Kept));
                    columns[2].Add(new StringValue(row.Text));
                    columns[3].Add(NullValue.Instance);
                    weights.Add(1);
                    iterations.Add(0);
                }
                yield return new ColumnReadEvent(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)), ++_watermark);
            }

            protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
            {
                await Task.CompletedTask;
                yield break;
            }

            public async Task<(bool Found, EventBatchData? Batch)> LookupBatchPublic(string key)
            {
                var columns = new IColumn[]
                {
                    Column.Create(GlobalMemoryManager.Instance),
                    Column.Create(GlobalMemoryManager.Instance),
                    Column.Create(GlobalMemoryManager.Instance),
                    Column.Create(GlobalMemoryManager.Instance)
                };
                using var keyBatch = new EventBatchData(columns);
                columns[0].Add(new StringValue(key));
                columns[1].Add(NullValue.Instance);
                columns[2].Add(NullValue.Instance);
                columns[3].Add(NullValue.Instance);
                var (found, row) = await LookupRowValue(new ColumnRowReference() { referenceBatch = keyBatch, RowIndex = 0 });
                return (found, found ? row.referenceBatch : null);
            }
        }

        private sealed class ImmediateReturnTarget : ITargetBlock<IStreamEvent>
        {
            private readonly CountingAllocator? _keyAllocator;
            private readonly TaskCompletionSource _completion = new TaskCompletionSource();

            public ImmediateReturnTarget(CountingAllocator? keyAllocator = null)
            {
                _keyAllocator = keyAllocator;
            }

            public List<(int Weight, long KeyAllocatorBytesBeforeRead, string Key)> Observed { get; } = new List<(int, long, string)>();

            public SemaphoreSlim Checkpoints { get; } = new SemaphoreSlim(0);

            public Task Completion => _completion.Task;

            public void Complete() => _completion.TrySetResult();

            public void Fault(Exception exception) => _completion.TrySetException(exception);

            public bool PostponeBatches { get; set; }

            public SemaphoreSlim Postponed { get; } = new SemaphoreSlim(0);

            private DataflowMessageHeader _postponedHeader;
            private ISourceBlock<IStreamEvent>? _postponedSource;

            public void DrainPostponed()
            {
                PostponeBatches = false;
                if (_postponedSource != null)
                {
                    var msg = _postponedSource.ConsumeMessage(_postponedHeader, this, out var consumed);
                    _postponedSource = null;
                    if (consumed && msg is StreamMessage<StreamEventBatch> batchMsg)
                    {
                        batchMsg.Data.Return();
                    }
                }
            }

            public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, IStreamEvent messageValue, ISourceBlock<IStreamEvent>? source, bool consumeToAccept)
            {
                if (PostponeBatches && messageValue is StreamMessage<StreamEventBatch> && source != null)
                {
                    _postponedHeader = messageHeader;
                    _postponedSource = source;
                    Postponed.Release();
                    return DataflowMessageStatus.Postponed;
                }
                if (consumeToAccept && source != null)
                {
                    messageValue = source.ConsumeMessage(messageHeader, this, out var consumed)!;
                    if (!consumed)
                    {
                        return DataflowMessageStatus.NotAvailable;
                    }
                }
                if (messageValue is StreamMessage<StreamEventBatch> batchMsg)
                {
                    try
                    {
                        var data = batchMsg.Data.Data;
                        long liveBytes = _keyAllocator?.OutstandingBytes ?? 0;
                        for (int i = 0; i < data.Count; i++)
                        {
                            var col = data.EventBatchData.Columns[0].GetValueAt(i, default);
                            string key = col.Type == ArrowTypeId.String ? col.AsString.ToString() : col.AsLong.ToString();
                            Observed.Add((data.Weights.Get(i), liveBytes, key));
                        }
                    }
                    finally
                    {
                        batchMsg.Data.Return();
                    }
                }
                else if (messageValue is ICheckpointEvent)
                {
                    Checkpoints.Release();
                }
                return DataflowMessageStatus.Accepted;
            }
        }

        private static async Task InitializeDirect(ColumnBatchReadBaseOperator op, ImmediateReturnTarget target, StateManagerSync<StreamState> manager, Meter meter)
        {
            op.LinkTo(target, new DataflowLinkOptions() { PropagateCompletion = true });
            op.Setup("stream", "source");
            op.CreateBlock();
            op.Link();
            ((IStreamIngressVertex)op).SetDependenciesDoneFunction(_ => { });

            var metrics = new StreamMetrics("stream");
            var handler = new VertexHandler(
                "stream",
                "source",
                (time, version) => { },
                (name, trigger, state) => Task.CompletedTask,
                metrics.GetOrCreateVertexMeter("source", () => ""),
                manager.GetOrCreateClient("source"),
                NullLoggerFactory.Instance,
                new OperatorMemoryManager("stream", "source", meter, new StreamMemoryManager("stream")),
                (ex, version) => Task.CompletedTask);

            await op.Initialize("source", 0, 0, handler, null);
        }

        private static ReadRelation HostedRelation()
        {
            var relation = new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { "t" } },
                BaseSchema = new NamedStruct()
                {
                    Names = new List<string>() { "k", "kept", "text" },
                    Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType(), new AnyType(), new AnyType() } }
                }
            };
            relation.RowStore = new RowStoreDefinition() { Name = "rows", StoredColumns = [2] };
            return relation;
        }

        private static ReadRelation GenericRelation()
        {
            var relation = new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { "items" } },
                BaseSchema = new NamedStruct()
                {
                    Names = new List<string>() { "Id", "Text", "__key" },
                    Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType(), new AnyType(), new AnyType() } }
                }
            };
            relation.RowStore = new RowStoreDefinition() { Name = "rows", StoredColumns = [1] };
            return relation;
        }

        private static EventBatchData OffsetBatch(IReadOnlyList<Row> innerRows, IReadOnlyList<int> order)
        {
            var allocator = GlobalMemoryManager.Instance;
            var raw = new Column[4];
            for (int c = 0; c < raw.Length; c++)
            {
                raw[c] = Column.Create(allocator);
            }
            foreach (var row in innerRows)
            {
                raw[0].Add(new Int64Value(row.Key));
                raw[1].Add(new Int64Value(row.Kept));
                raw[2].Add(new StringValue(row.Text));
                raw[3].Add(new Int64Value(row.Number));
            }
            var wrapped = new IColumn[4];
            for (int c = 0; c < wrapped.Length; c++)
            {
                var offsets = new PrimitiveList<int>(allocator);
                foreach (int index in order)
                {
                    offsets.Add(index);
                }
                wrapped[c] = new ColumnWithOffset(raw[c], offsets);
            }
            return new EventBatchData(wrapped);
        }

        private static async Task<Row?> LookupRow(RowStoreHost host, long key)
        {
            var columns = new IColumn[4];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
            }
            using var batch = new EventBatchData(columns);
            columns[0].Add(new Int64Value(key));
            for (int i = 1; i < columns.Length; i++)
            {
                columns[i].Add(NullValue.Instance);
            }
            var found = await host.LookupAsync(new ColumnRowReference() { referenceBatch = batch, RowIndex = 0 });
            if (found == null)
            {
                return null;
            }
            try
            {
                return new Row(
                    found[0].GetValueAt(0, default).AsLong,
                    found[1].GetValueAt(0, default).AsLong,
                    found[2].GetValueAt(0, default).AsString.ToString(),
                    found[3].GetValueAt(0, default).AsLong);
            }
            finally
            {
                foreach (var column in found)
                {
                    column.Dispose();
                }
            }
        }

        [Fact]
        public async Task ApplyAsyncSupportsColumnWithOffsetInputs()
        {
            using var stream = await Open();
            var layout = new RowStoreHostLayout(4, [0], [1], [2, 3]);
            var options = new RowStoreOptions() { Mode = RowStoreMode.Keyed, Heap = new RowHeapOptions() { PageTargetBytes = 2000 } };
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, options, GlobalMemoryManager.Instance);

            using var firstBatch = OffsetBatch([new Row(2, 20, "b", 200), new Row(1, 10, "a", 100)], [1, 0]);
            using var firstWeights = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 1, 1 };
            using var firstResult = await host.ApplyAsync(firstBatch, firstWeights, null);
            Assert.Equal(2, firstResult.InsertRows.Count);

            using var secondBatch = OffsetBatch([new Row(2, 20, "b-updated", 201), new Row(1, 10, "a", 100)], [1, 0]);
            using var secondWeights = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 1, 1 };
            using var secondResult = await host.ApplyAsync(secondBatch, secondWeights, null);
            Assert.Single(secondResult.InsertRows);
            Assert.Single(secondResult.DeleteRows);
            Assert.Equal(new Row(1, 10, "a", 100), await LookupRow(host, 1));
            Assert.Equal(new Row(2, 20, "b-updated", 201), await LookupRow(host, 2));
            foreach (var col in firstBatch.Columns)
            {
                ((ColumnWithOffset)col).Offsets.Dispose();
            }
            foreach (var col in secondBatch.Columns)
            {
                ((ColumnWithOffset)col).Offsets.Dispose();
            }
        }

        [Fact]
        public async Task HostedGenericReadMixedDeleteThenUpsertPreservesPayloadRowAlignment()
        {
            using var stream = await Open();
            var layout = new RowStoreHostLayout(3, [2], [0], [1]);
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout,
                new RowStoreOptions() { Mode = RowStoreMode.Keyed }, GlobalMemoryManager.Instance);
            var op = new InspectableGenericReadOperator(new GatedGenericDataSource([], []), GenericRelation());
            var columns = new IColumn[3];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
            }
            using var batch = new EventBatchData(columns);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            using var iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
            var append = typeof(GenericReadOperator<GenericItem>).GetMethod("AppendToColumns",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
            append.Invoke(op, [columns, weights, iterations, new FlowtideGenericObject<GenericItem>("k1", null, 2, isDelete: true)]);
            append.Invoke(op, [columns, weights, iterations, new FlowtideGenericObject<GenericItem>("k2", new GenericItem() { Id = 2, Text = "second" }, 3, isDelete: false)]);

            using var result = await host.ApplyAsync(batch, weights, null);

            Assert.Equal(1, result.InsertRows.Count);
            Assert.Equal(1, result.InsertRows[0]);
            var row = await host.LookupAsync(new ColumnRowReference() { referenceBatch = batch, RowIndex = 1 });
            Assert.NotNull(row);
            try
            {
                Assert.Equal(2, row[0].GetValueAt(0, default).AsLong);
                Assert.Equal("second", row[1].GetValueAt(0, default).AsString.ToString());
            }
            finally
            {
                foreach (var column in row)
                {
                    column.Dispose();
                }
            }
        }

        [Fact]
        public async Task HostedGenericReadDeltaDeleteDoesNotCrashOnUnpopulatedDeferredColumns()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            using var meter = new Meter("generic-delete");
            using var manager = new StateManagerSync<StreamState>(
                new StateManagerOptions() { PersistentStorage = storage },
                NullLoggerFactory.Instance,
                meter,
                "generic-delete",
                GlobalMemoryManager.Instance);
            await manager.InitializeAsync();

            var dataSource = new GatedGenericDataSource(
                [new FlowtideGenericObject<GenericItem>("k1", new GenericItem() { Id = 1, Text = "hello" }, 1, isDelete: false)],
                [new FlowtideGenericObject<GenericItem>("k1", null, 2, isDelete: true)]);
            var op = new InspectableGenericReadOperator(dataSource, GenericRelation());
            var target = new ImmediateReturnTarget();

            await InitializeDirect(op, target, manager, meter);
            await op.InitializationCompleted();
            Assert.True(await dataSource.FullEntered.WaitAsync(Limit));
            op.DoLockingEvent(new Checkpoint(1, 2, 1));
            Assert.True(await target.Checkpoints.WaitAsync(Limit));
            await op.BeforeSaveCheckpoint();
            await manager.CheckpointAsync();

            await op.OnTrigger("delta_load", null);
            Assert.True(await dataSource.DeltaEntered.WaitAsync(Limit));
            op.DoLockingEvent(new Checkpoint(2, 3, 2));
            var completed = await Task.WhenAny(target.Checkpoints.WaitAsync(Limit), target.Completion);
            if (completed == target.Completion)
            {
                await target.Completion;
            }
            Assert.Null(await op.LookupPublic("k1"));
            await op.DisposeAsync();
        }

        [Fact]
        public async Task HostedGenericReadDeltaWithADeleteBeforeUpsertsStoresEachRowUnderItsKey()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            using var meter = new Meter("generic-mixed");
            using var manager = new StateManagerSync<StreamState>(
                new StateManagerOptions() { PersistentStorage = storage },
                NullLoggerFactory.Instance,
                meter,
                "generic-mixed",
                GlobalMemoryManager.Instance);
            await manager.InitializeAsync();

            var dataSource = new GatedGenericDataSource(
                [
                    new FlowtideGenericObject<GenericItem>("k1", new GenericItem() { Id = 1, Text = "one" }, 1, isDelete: false),
                    new FlowtideGenericObject<GenericItem>("k2", new GenericItem() { Id = 2, Text = "two" }, 1, isDelete: false)
                ],
                [
                    new FlowtideGenericObject<GenericItem>("k1", null, 2, isDelete: true),
                    new FlowtideGenericObject<GenericItem>("k2", new GenericItem() { Id = 2, Text = "two-new" }, 3, isDelete: false),
                    new FlowtideGenericObject<GenericItem>("k3", new GenericItem() { Id = 3, Text = "three" }, 4, isDelete: false)
                ]);
            var op = new InspectableGenericReadOperator(dataSource, GenericRelation());
            var target = new ImmediateReturnTarget();

            await InitializeDirect(op, target, manager, meter);
            await op.InitializationCompleted();
            Assert.True(await dataSource.FullEntered.WaitAsync(Limit));
            op.DoLockingEvent(new Checkpoint(1, 2, 1));
            Assert.True(await target.Checkpoints.WaitAsync(Limit));
            await op.BeforeSaveCheckpoint();
            await manager.CheckpointAsync();

            await op.OnTrigger("delta_load", null);
            Assert.True(await dataSource.DeltaEntered.WaitAsync(Limit));
            op.DoLockingEvent(new Checkpoint(2, 3, 2));
            var completed = await Task.WhenAny(target.Checkpoints.WaitAsync(Limit), target.Completion);
            if (completed == target.Completion)
            {
                await target.Completion;
            }
            Assert.Null(await op.LookupPublic("k1"));
            Assert.Equal("two-new", (await op.LookupPublic("k2"))?.Text);
            Assert.Equal("three", (await op.LookupPublic("k3"))?.Text);
            await op.DisposeAsync();
        }

        [Fact]
        public async Task ConcurrentGenericReadLookupsDoNotOverwriteSharedKeyBuffer()
        {
            var provider = new GatedProvider();
            var initialRows = new List<FlowtideGenericObject<GenericItem>>()
            {
                new FlowtideGenericObject<GenericItem>("k1", new GenericItem() { Id = 1, Text = "one-" + new string('a', 40000) }, 1, isDelete: false),
                new FlowtideGenericObject<GenericItem>("k2", new GenericItem() { Id = 2, Text = "two-" + new string('b', 40000) }, 2, isDelete: false)
            };

            using (var firstStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = provider, MaxFileSize = 4096 }))
            using (var firstMeter = new Meter("generic-lookup-1"))
            using (var firstManager = new StateManagerSync<StreamState>(
                new StateManagerOptions() { PersistentStorage = firstStorage },
                NullLoggerFactory.Instance,
                firstMeter,
                "generic-lookup",
                GlobalMemoryManager.Instance))
            {
                await firstManager.InitializeAsync();
                var fullSource = new GatedGenericDataSource(initialRows, []);
                var op1 = new InspectableGenericReadOperator(fullSource, GenericRelation());
                var target1 = new ImmediateReturnTarget();
                await InitializeDirect(op1, target1, firstManager, firstMeter);
                await op1.InitializationCompleted();
                Assert.True(await fullSource.FullEntered.WaitAsync(Limit));
                op1.DoLockingEvent(new Checkpoint(1, 2, 1));
                Assert.True(await target1.Checkpoints.WaitAsync(Limit));
                await op1.BeforeSaveCheckpoint();
                await firstManager.CheckpointAsync();
                await op1.DisposeAsync();
            }

            using var secondStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = provider });
            using var secondMeter = new Meter("generic-lookup-2");
            using var secondManager = new StateManagerSync<StreamState>(
                new StateManagerOptions() { PersistentStorage = secondStorage },
                NullLoggerFactory.Instance,
                secondMeter,
                "generic-lookup",
                GlobalMemoryManager.Instance);
            await secondManager.InitializeAsync();

            var op2 = new InspectableGenericReadOperator(new GatedGenericDataSource(initialRows, []), GenericRelation());
            var target2 = new ImmediateReturnTarget();
            await InitializeDirect(op2, target2, secondManager, secondMeter);

            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            int gated = 0;
            provider.OnReadAsync = async () =>
            {
                if (Interlocked.Exchange(ref gated, 1) == 0)
                {
                    entered.TrySetResult();
                    await release.Task;
                }
            };

            var lookupFirst = op2.LookupPublic("k1").AsTask();
            await entered.Task.WaitAsync(Limit);

            var lookupSecond = op2.LookupPublic("k2").AsTask();
            var lookupThird = op2.LookupPublic("k1").AsTask();
            provider.OnReadAsync = null;
            release.TrySetResult();

            var firstItem = await lookupFirst.WaitAsync(Limit);
            var secondItem = await lookupSecond.WaitAsync(Limit);
            var thirdItem = await lookupThird.WaitAsync(Limit);

            Assert.StartsWith("one-", firstItem?.Text);
            Assert.StartsWith("two-", secondItem?.Text);
            Assert.StartsWith("one-", thirdItem?.Text);
            await op2.DisposeAsync();
        }

        [Fact]
        public async Task HostedFullReloadDoesNotLeakInputColumnsWhenBatchIsUnchanged()
        {
            var keyAllocator = new CountingAllocator();
            var restAllocator = new CountingAllocator();
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            using var meter = new Meter("unchanged-reload");
            using var manager = new StateManagerSync<StreamState>(
                new StateManagerOptions() { PersistentStorage = storage },
                NullLoggerFactory.Instance,
                meter,
                "unchanged-reload",
                GlobalMemoryManager.Instance);
            await manager.InitializeAsync();

            var rows = new List<(string Key, long Kept, string Text)>()
            {
                ("k1", 10, "alpha"),
                ("k2", 20, "beta")
            };
            var source = new CountingSource(HostedRelation(), rows, keyAllocator, restAllocator, [0]);
            var target = new ImmediateReturnTarget(keyAllocator);

            await InitializeDirect(source, target, manager, meter);
            await source.InitializationCompleted();
            Assert.True(await source.LoadEntered.WaitAsync(Limit));
            source.DoLockingEvent(new Checkpoint(1, 2, 1));
            Assert.True(await target.Checkpoints.WaitAsync(Limit));
            await source.BeforeSaveCheckpoint();
            await manager.CheckpointAsync();

            await source.OnTrigger("full_load", null);
            Assert.True(await source.LoadEntered.WaitAsync(Limit));
            source.DoLockingEvent(new Checkpoint(2, 3, 2));
            Assert.True(await target.Checkpoints.WaitAsync(Limit));
            await source.DisposeAsync();

            Assert.Equal(0, keyAllocator.BytesFreedByFinalizers);
            Assert.Equal(0, restAllocator.BytesFreedByFinalizers);
            Assert.Equal(0, keyAllocator.OutstandingBytes);
            Assert.Equal(0, restAllocator.OutstandingBytes);
        }

        [Fact]
        public async Task LookupRowValueHostedCopiesDuplicateEmitColumns()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            using var meter = new Meter("duplicate-emit");
            using var manager = new StateManagerSync<StreamState>(
                new StateManagerOptions() { PersistentStorage = storage },
                NullLoggerFactory.Instance,
                meter,
                "duplicate-emit",
                GlobalMemoryManager.Instance);
            await manager.InitializeAsync();

            var relation = new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { "t" } },
                BaseSchema = new NamedStruct()
                {
                    Names = new List<string>() { "k", "kept", "text", "unused" },
                    Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType(), new AnyType(), new AnyType(), new AnyType() } }
                },
                Emit = new List<int>() { 0, 1, 1, 2 }
            };
            relation.RowStore = new RowStoreDefinition() { Name = "rows", StoredColumns = [3] };

            var rows = new List<(string Key, long Kept, string Text)>() { ("key-1", 10, "alpha") };
            var source = new CountingSource(relation, rows, GlobalMemoryManager.Instance, GlobalMemoryManager.Instance, [0]);
            var target = new ImmediateReturnTarget();

            await InitializeDirect(source, target, manager, meter);
            await source.InitializationCompleted();
            Assert.True(await source.LoadEntered.WaitAsync(Limit));
            source.DoLockingEvent(new Checkpoint(1, 2, 1));
            Assert.True(await target.Checkpoints.WaitAsync(Limit));

            var (found, batch) = await source.LookupBatchPublic("key-1");
            Assert.True(found);
            Assert.NotNull(batch);
            try
            {
                Assert.NotSame(batch!.Columns[0], batch.Columns[1]);
            }
            finally
            {
                batch!.Dispose();
            }
            await source.DisposeAsync();
        }

        [Fact]
        public async Task FailedApplyAsyncPoisonsHostAgainstSubsequentCalls()
        {
            using var stream = await Open();
            var layout = new RowStoreHostLayout(4, [0], [1], [2, 3]);
            var options = new RowStoreOptions() { Mode = RowStoreMode.Keyed, Heap = new RowHeapOptions() { PageTargetBytes = 2000 } };
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, options, GlobalMemoryManager.Instance);

            var columns = new IColumn[4];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
            }
            using var batch = new EventBatchData(columns);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            foreach (var row in new[] { new Row(1, 10, "a", 1), new Row(2, 20, "b", 2), new Row(1, 10, "c", 1) })
            {
                columns[0].Add(new Int64Value(row.Key));
                columns[1].Add(new Int64Value(row.Kept));
                columns[2].Add(new StringValue(row.Text));
                columns[3].Add(new Int64Value(row.Number));
                weights.Add(row.Weight);
            }

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                host.ApplyAsync(batch, weights, (_, rowIndex) => rowIndex == 2 ? throw new InvalidOperationException("boom") : true).AsTask());

            await Assert.ThrowsAsync<InvalidOperationException>(() => host.BeginCheckpointAsync());
        }

        [Fact]
        public async Task LookupAsyncDoesNotDirtyLeavesInMultiLeafKeyTree()
        {
            using var stream = await Open();
            var layout = new RowStoreHostLayout(4, [0], [1], [2, 3]);
            var options = new RowStoreOptions() { Mode = RowStoreMode.Keyed, Heap = new RowHeapOptions() { PageTargetBytes = 2000 } };
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, options, GlobalMemoryManager.Instance);

            var columns = new IColumn[4];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
            }
            using var batch = new EventBatchData(columns);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            for (int i = 0; i < 1500; i++)
            {
                columns[0].Add(new Int64Value(i));
                columns[1].Add(new Int64Value(i * 100000L));
                columns[2].Add(new StringValue("text" + i));
                columns[3].Add(new Int64Value(i * 100));
                weights.Add(1);
            }
            using (await host.ApplyAsync(batch, weights, null))
            {
            }
            await host.BeginCheckpointAsync();
            await host.CheckpointAsync();
            await stream.Manager.CheckpointAsync();

            ulong commitsBeforeLookup = stream.Manager.PageCommits;
            Assert.NotNull(await LookupRow(host, 700));
            Assert.Null(await LookupRow(host, 99999));

            await host.BeginCheckpointAsync();
            await host.CheckpointAsync();
            await stream.Manager.CheckpointAsync();

            Assert.Equal(commitsBeforeLookup, stream.Manager.PageCommits);
        }

        [Fact]
        public async Task ApplySortedAsyncSplitsLeafWhenInsertedRowExceedsPageSizeInMostlyUnchangedBatch()
        {
            using var stream = await Open();
            var layout = new RowStoreHostLayout(4, [0], [1, 2], [3]);
            var options = new RowStoreOptions() { Mode = RowStoreMode.Keyed, Heap = new RowHeapOptions() { PageTargetBytes = 2000 } };
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, options, GlobalMemoryManager.Instance);

            var initialColumns = new IColumn[4];
            for (int i = 0; i < initialColumns.Length; i++)
            {
                initialColumns[i] = Column.Create(GlobalMemoryManager.Instance);
            }
            using var initialBatch = new EventBatchData(initialColumns);
            using var initialWeights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            for (int i = 0; i < 120; i++)
            {
                initialColumns[0].Add(new Int64Value(i));
                initialColumns[1].Add(new Int64Value(i));
                initialColumns[2].Add(new StringValue(new string('k', 80)));
                initialColumns[3].Add(new Int64Value(i));
                initialWeights.Add(1);
            }
            using (await host.ApplyAsync(initialBatch, initialWeights, null))
            {
            }

            var nextColumns = new IColumn[4];
            for (int i = 0; i < nextColumns.Length; i++)
            {
                nextColumns[i] = Column.Create(GlobalMemoryManager.Instance);
            }
            using var nextBatch = new EventBatchData(nextColumns);
            using var nextWeights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            for (int i = 0; i < 120; i++)
            {
                nextColumns[0].Add(new Int64Value(i));
                nextColumns[1].Add(new Int64Value(i));
                nextColumns[2].Add(new StringValue(new string('k', 80)));
                nextColumns[3].Add(new Int64Value(i));
                nextWeights.Add(1);
            }
            nextColumns[0].Add(new Int64Value(999));
            nextColumns[1].Add(new Int64Value(999));
            nextColumns[2].Add(new StringValue(new string('w', 30000)));
            nextColumns[3].Add(new Int64Value(999));
            nextWeights.Add(1);

            using (await host.ApplyAsync(nextBatch, nextWeights, null))
            {
            }

            int leaves = 0;
            using var iterator = host.CreateKeyIterator();
            await iterator.SeekFirst();
            await foreach (var _ in iterator)
            {
                leaves++;
            }
            Assert.True(leaves >= 2);
        }

        [Fact]
        public async Task CreateAsyncDisposesRowStoreWhenReaderWindowCreationFails()
        {
            using var stream = await Open(cachePages: 340);
            var layout = new RowStoreHostLayout(3, [0], [1], [2]);

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                RowStoreHost.CreateAsync(stream.Client, "keys1", "rows1", layout, new RowStoreOptions(), GlobalMemoryManager.Instance));

            using var heap = await RowHeap.CreateAsync(stream.Client, "rows1", 1, new RowHeapOptions(), GlobalMemoryManager.Instance);
            Assert.Equal(0, heap.RowCount);
        }

        [Fact]
        public async Task DisposeReleasesSearcherLeafRentsAfterMidSearchException()
        {
            var hostAllocator = new CountingAllocator();
            using (var stream = await Open())
            {
                var layout = new RowStoreHostLayout(4, [0], [1], [2, 3]);
                var options = new RowStoreOptions() { Mode = RowStoreMode.Keyed, Heap = new RowHeapOptions() { PageTargetBytes = 2000 } };
                using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, options, hostAllocator);

                var loadColumns = new IColumn[4];
                for (int i = 0; i < loadColumns.Length; i++)
                {
                    loadColumns[i] = Column.Create(GlobalMemoryManager.Instance);
                }
                using (var loadBatch = new EventBatchData(loadColumns))
                using (var loadWeights = new PrimitiveList<int>(GlobalMemoryManager.Instance))
                {
                    for (int i = 0; i < 1500; i++)
                    {
                        loadColumns[0].Add(new Int64Value(i));
                        loadColumns[1].Add(new Int64Value(i * 100000L));
                        loadColumns[2].Add(new StringValue("text" + i));
                        loadColumns[3].Add(new Int64Value(i));
                        loadWeights.Add(1);
                    }
                    using (await host.ApplyAsync(loadBatch, loadWeights, null))
                    {
                    }
                }

                var failColumns = new IColumn[]
                {
                    Column.Create(GlobalMemoryManager.Instance),
                    new ThrowingKeptColumn(),
                    Column.Create(GlobalMemoryManager.Instance),
                    Column.Create(GlobalMemoryManager.Instance)
                };
                using var failBatch = new EventBatchData(failColumns);
                using var failWeights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
                failColumns[0].Add(new Int64Value(0));
                ((Column)failColumns[1]).Add(new Int64Value(0));
                failColumns[2].Add(new StringValue("text0"));
                failColumns[3].Add(new Int64Value(0));
                failWeights.Add(1);

                await Assert.ThrowsAsync<InvalidOperationException>(() => host.ApplyAsync(failBatch, failWeights, null).AsTask());
            }

            Assert.Equal(0, hostAllocator.BytesFreedByFinalizers);
            Assert.Equal(0, hostAllocator.OutstandingBytes);
        }

        [Fact]
        public async Task KeyedHostRejectsEmptyKeyColumnsInsteadOfThrowingIndexOutOfRange()
        {
            using var stream = await Open();
            var layout = new RowStoreHostLayout(2, [], [0], [1]);
            var options = new RowStoreOptions() { Mode = RowStoreMode.Keyed };

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, options, GlobalMemoryManager.Instance));
        }

        [Fact]
        public void ReadLayoutAllowsDeferringRepeatedOutputPositions()
        {
            var layout = RowStoreHost.ReadLayout(new RowStoreDefinition() { Name = "rows", StoredColumns = [1, 2] }, 3, [0], [0, 2, 2]);
            Assert.NotNull(layout);
            Assert.Equal([2], layout!.DeferredColumns);
            Assert.Empty(layout.KeptColumns);
        }

        [Fact]
        public async Task HostedGenericReadMixedDeleteAndUpsertBatchDoesNotMisalignOrCrash()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            using var meter = new Meter("generic-mixed-delta");
            using var manager = new StateManagerSync<StreamState>(
                new StateManagerOptions() { PersistentStorage = storage },
                NullLoggerFactory.Instance,
                meter,
                "generic-mixed-delta",
                GlobalMemoryManager.Instance);
            await manager.InitializeAsync();

            var dataSource = new GatedGenericDataSource(
                [new FlowtideGenericObject<GenericItem>("k1", new GenericItem() { Id = 1, Text = "one" }, 1, isDelete: false)],
                [
                    new FlowtideGenericObject<GenericItem>("k1", null, 2, isDelete: true),
                    new FlowtideGenericObject<GenericItem>("k2", new GenericItem() { Id = 2, Text = "two" }, 3, isDelete: false)
                ]);
            var op = new InspectableGenericReadOperator(dataSource, GenericRelation());
            var target = new ImmediateReturnTarget();

            await InitializeDirect(op, target, manager, meter);
            await op.InitializationCompleted();
            Assert.True(await dataSource.FullEntered.WaitAsync(Limit));
            op.DoLockingEvent(new Checkpoint(1, 2, 1));
            Assert.True(await target.Checkpoints.WaitAsync(Limit));
            await op.BeforeSaveCheckpoint();
            await manager.CheckpointAsync();

            await op.OnTrigger("delta_load", null);
            Assert.True(await dataSource.DeltaEntered.WaitAsync(Limit));
            op.DoLockingEvent(new Checkpoint(2, 3, 2));
            var completed = await Task.WhenAny(target.Checkpoints.WaitAsync(Limit), target.Completion);
            if (completed == target.Completion)
            {
                await target.Completion;
            }
            Assert.Null(await op.LookupPublic("k1"));
            var k2 = await op.LookupPublic("k2");
            Assert.NotNull(k2);
            Assert.Equal(2, k2!.Id);
            Assert.Equal("two", k2.Text);
            await op.DisposeAsync();
        }

        [Fact]
        public async Task ParallelMultiThreadedLookupAsyncCallsDoNotRaceOnSharedLookupContainer()
        {
            using var stream = await Open();
            var layout = new RowStoreHostLayout(4, [0], [1], [2, 3]);
            var options = new RowStoreOptions() { Mode = RowStoreMode.Keyed, Heap = new RowHeapOptions() { PageTargetBytes = 2000 } };
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, options, GlobalMemoryManager.Instance);

            var columns = new IColumn[4];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
            }
            using (var batch = new EventBatchData(columns))
            using (var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance))
            {
                for (int i = 0; i < 64; i++)
                {
                    columns[0].Add(new StringValue("key-" + i));
                    columns[1].Add(new Int64Value(i));
                    columns[2].Add(new StringValue("val-" + i));
                    columns[3].Add(new Int64Value(i * 10));
                    weights.Add(1);
                }
                using (await host.ApplyAsync(batch, weights, null))
                {
                }
            }

            using var barrier = new Barrier(8);
            var tasks = Enumerable.Range(0, 8).Select(worker => Task.Run(async () =>
            {
                var keyCols = new IColumn[4];
                for (int c = 0; c < 4; c++)
                {
                    keyCols[c] = Column.Create(GlobalMemoryManager.Instance);
                }
                using var keyBatch = new EventBatchData(keyCols);
                if (worker % 2 == 0)
                {
                    keyCols[0].Add(new StringValue("key-" + worker));
                }
                else
                {
                    keyCols[0].Add(new Int64Value(worker));
                }
                keyCols[1].Add(NullValue.Instance);
                keyCols[2].Add(NullValue.Instance);
                keyCols[3].Add(NullValue.Instance);

                barrier.SignalAndWait(Limit);
                for (int iter = 0; iter < 500; iter++)
                {
                    var found = await host.LookupAsync(new ColumnRowReference() { referenceBatch = keyBatch, RowIndex = 0 });
                    if (worker % 2 == 0)
                    {
                        Assert.NotNull(found);
                        try
                        {
                            Assert.Equal("key-" + worker, found![0].GetValueAt(0, default).AsString.ToString());
                            Assert.Equal(worker, found[1].GetValueAt(0, default).AsLong);
                        }
                        finally
                        {
                            foreach (var col in found!)
                            {
                                col.Dispose();
                            }
                        }
                    }
                    else
                    {
                        if (found != null)
                        {
                            foreach (var col in found)
                            {
                                col.Dispose();
                            }
                        }
                        Assert.Null(found);
                    }
                }
            })).ToArray();

            await Task.WhenAll(tasks).WaitAsync(Limit);
        }

        [Fact]
        public async Task LookupManyAsyncWithZeroCountDoesNotCrashOrPoisonHost()
        {
            using var stream = await Open();
            var layout = new RowStoreHostLayout(4, [0], [1], [2, 3]);
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, new RowStoreOptions() { Mode = RowStoreMode.Keyed }, GlobalMemoryManager.Instance);
            using var emptyKeys = new EventBatchData([Column.Create(GlobalMemoryManager.Instance)]);

            var (rows, found) = await host.LookupManyAsync(emptyKeys, 0);

            Assert.Null(rows);
            Assert.Equal(0, found);
            await host.BeginCheckpointAsync();
            await host.CheckpointAsync();
        }

        [Fact]
        public async Task DuplicateKeyFollowedByZeroWeightRowDoesNotDropUpdateWhenZeroWeightDoesNotDelete()
        {
            using var stream = await Open();
            var layout = new RowStoreHostLayout(4, [0], [1], [2, 3]);
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, new RowStoreOptions() { Mode = RowStoreMode.Keyed }, GlobalMemoryManager.Instance, zeroWeightDeletes: false);

            var columns = new IColumn[4];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
            }
            using var batch = new EventBatchData(columns);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            columns[0].Add(new Int64Value(1));
            columns[1].Add(new Int64Value(10));
            columns[2].Add(new StringValue("inserted"));
            columns[3].Add(new Int64Value(100));
            weights.Add(1);

            columns[0].Add(new Int64Value(1));
            columns[1].Add(new Int64Value(99));
            columns[2].Add(new StringValue("ignored-zero"));
            columns[3].Add(new Int64Value(999));
            weights.Add(0);

            using var result = await host.ApplyAsync(batch, weights, null);

            Assert.Single(result.InsertRows);
            Assert.Equal(new Row(1, 10, "inserted", 100), await LookupRow(host, 1));
        }

        [Fact]
        public async Task LayoutRecordRejectsSwappedDeferredPositionsOnRepeatedEmitColumns()
        {
            using var stream = await Open();
            var emit = new List<int>() { 0, 2, 2 };
            var firstLayout = RowStoreHost.ReadLayout(new RowStoreDefinition() { Name = "rows", StoredColumns = [1] }, 3, [0], emit);
            Assert.NotNull(firstLayout);
            using (var firstHost = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", firstLayout!, new RowStoreOptions() { Mode = RowStoreMode.Keyed }, GlobalMemoryManager.Instance))
            {
                await firstHost.BeginCheckpointAsync();
                await firstHost.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
            }

            var secondLayout = RowStoreHost.ReadLayout(new RowStoreDefinition() { Name = "rows", StoredColumns = [2] }, 3, [0], emit);
            Assert.NotNull(secondLayout);
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                RowStoreHost.CreateAsync(stream.Client, "keys", "rows", secondLayout!, new RowStoreOptions() { Mode = RowStoreMode.Keyed }, GlobalMemoryManager.Instance));
        }

        [Fact]
        public async Task ApplyAsyncSupportsAlwaysNullColumnInputs()
        {
            using var stream = await Open();
            var layout = new RowStoreHostLayout(3, [0], [1], [2]);
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, new RowStoreOptions() { Mode = RowStoreMode.Keyed }, GlobalMemoryManager.Instance);

            using var keys = Column.Create(GlobalMemoryManager.Instance);
            keys.Add(new Int64Value(1));
            keys.Add(new Int64Value(2));
            var columns = new IColumn[] { keys, AlwaysNullColumn.Instance, AlwaysNullColumn.Instance };
            var batch = new EventBatchData(columns);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 1, 1 };

            using var result = await host.ApplyAsync(batch, weights, null);
            Assert.Equal(2, result.InsertRows.Count);
        }

        [Fact]
        public async Task NormalizeBulkInsertSupportsAlwaysNullColumn()
        {
            using var stream = await Open();
            var layout = new RowStoreHostLayout(3, [0], [1], [2]);
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, new RowStoreOptions() { Mode = RowStoreMode.Keyed }, GlobalMemoryManager.Instance);

            using var keys = Column.Create(GlobalMemoryManager.Instance);
            using var deferred = Column.Create(GlobalMemoryManager.Instance);
            keys.Add(new Int64Value(1));
            keys.Add(new Int64Value(2));
            deferred.Add(new StringValue("a"));
            deferred.Add(new StringValue("b"));
            var batch = new EventBatchData([keys, AlwaysNullColumn.Instance, deferred]);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 1, 1 };

            using var result = await host.ApplyAsync(batch, weights, null);
            Assert.Equal(2, result.InsertRows.Count);
        }

        [Fact]
        public async Task LookupAsyncSetsRowCountOnUnemittedSchemaColumns()
        {
            using var stream = await Open();
            var layout = RowStoreHost.ReadLayout(new RowStoreDefinition() { Name = "rows", StoredColumns = [2] }, 4, [1], [1, 2, 3]);
            Assert.NotNull(layout);
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout!, new RowStoreOptions() { Mode = RowStoreMode.Keyed }, GlobalMemoryManager.Instance);

            var cols = new IColumn[4];
            for (int c = 0; c < 4; c++)
            {
                cols[c] = Column.Create(GlobalMemoryManager.Instance);
            }
            using var batch = new EventBatchData(cols);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 1 };
            cols[0].Add(new Int64Value(999));
            cols[1].Add(new Int64Value(1));
            cols[2].Add(new Int64Value(10));
            cols[3].Add(new StringValue("payload"));
            using (await host.ApplyAsync(batch, weights, null))
            {
            }

            var found = await host.LookupAsync(new ColumnRowReference() { referenceBatch = batch, RowIndex = 0 });
            Assert.NotNull(found);
            try
            {
                Assert.True(new EventBatchData(found!).Count == 1);
                Assert.All(found!, col => Assert.True(col.Count == 1));
            }
            finally
            {
                foreach (var col in found!)
                {
                    col.Dispose();
                }
            }
        }

        [Fact]
        public async Task KeyOnlyDeleteWithTypedKeptColumnDoesNotOverreadKeptColumnPrefixSums()
        {
            using var stream = await Open();
            var layout = new RowStoreHostLayout(3, [0], [1], [2]);
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, new RowStoreOptions() { Mode = RowStoreMode.Keyed }, GlobalMemoryManager.Instance);

            var initCols = new IColumn[] { Column.Create(GlobalMemoryManager.Instance), Column.Create(GlobalMemoryManager.Instance), Column.Create(GlobalMemoryManager.Instance) };
            using (var initBatch = new EventBatchData(initCols))
            using (var initWeights = new PrimitiveList<int>(GlobalMemoryManager.Instance))
            {
                for (int i = 0; i < 80; i++)
                {
                    initCols[0].Add(new Int64Value(i));
                    initCols[1].Add(new StringValue("kept-" + i));
                    initCols[2].Add(new StringValue("payload-" + i));
                    initWeights.Add(1);
                }
                using (await host.ApplyAsync(initBatch, initWeights, null))
                {
                }
            }

            var delCols = new IColumn[] { Column.Create(GlobalMemoryManager.Instance), Column.Create(GlobalMemoryManager.Instance), Column.Create(GlobalMemoryManager.Instance) };
            using var delBatch = new EventBatchData(delCols);
            using var delWeights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            delCols[0].Add(new Int64Value(0));
            delCols[1].Add(new Int64Value(1));
            delCols[1].UpdateAt(0, new StringValue("union-kept"));
            delCols[2].Add(new StringValue("payload-0"));
            delWeights.Add(1);
            for (int i = 1; i < 80; i++)
            {
                delCols[0].Add(new Int64Value(i));
                delWeights.Add(-1);
            }

            using var result = await host.ApplyAsync(delBatch, delWeights, null);
            Assert.Equal(80, result.DeleteRows.Count);
        }

        [Fact]
        public async Task AppendOnlyHostCountsZeroWeightRowsWhenZeroWeightIsRowIsTrue()
        {
            using var stream = await Open();
            var layout = new RowStoreHostLayout(2, [], [0], [1]);
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, new RowStoreOptions() { Mode = RowStoreMode.Append }, GlobalMemoryManager.Instance);

            using var col0 = Column.Create(GlobalMemoryManager.Instance);
            using var col1 = Column.Create(GlobalMemoryManager.Instance);
            col0.Add(new Int64Value(1));
            col1.Add(new StringValue("zero-loaded"));
            using var batch = new EventBatchData([col0, col1]);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 0 };

            using var result = await host.ApplyAsync(batch, weights, null, zeroWeightIsRow: true);
            Assert.Single(result.InsertRows);
        }

        [Fact]
        public void NormalizeBulkInsertSupportsColumnWithOffsetWrappingAlwaysNullColumn()
        {
            using var storage = new FlowtideDotNet.Core.Operators.Normalization.NormalizeValueStorage([0], GlobalMemoryManager.Instance);
            using var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 0, 1 };
            var wrapped = new ColumnWithOffset(AlwaysNullColumn.Instance, offsets);
            var source = new EventBatchData([wrapped]);
            var values = new ColumnRowReference[]
            {
                new ColumnRowReference() { referenceBatch = source, RowIndex = 0 },
                new ColumnRowReference() { referenceBatch = source, RowIndex = 1 }
            };

            storage.InsertFrom(values, [0, 1], [0, 0]);
            Assert.Equal(2, storage.Count);
        }

        [Fact]
        public async Task CreateBatchesDoesNotWrapAlwaysNullColumnInUnsortableColumnWithOffset()
        {
            using var stream = await Open();
            var layout = new RowStoreHostLayout(3, [0], [1], [2]);
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, new RowStoreOptions() { Mode = RowStoreMode.Keyed }, GlobalMemoryManager.Instance);

            using var keys = Column.Create(GlobalMemoryManager.Instance);
            using var payload = Column.Create(GlobalMemoryManager.Instance);
            keys.Add(new Int64Value(2));
            keys.Add(new Int64Value(1));
            payload.Add(new StringValue("a"));
            payload.Add(new StringValue("b"));
            using var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 0, 1 };
            var batch = new EventBatchData([keys, new ColumnWithOffset(AlwaysNullColumn.Instance, offsets), payload]);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 1, 1 };

            var result = await host.ApplyAsync(batch, weights, null);
            var (inserts, deletes) = host.CreateBatches(batch, result, [0, 1, 2]);
            Assert.NotNull(inserts);
            try
            {
                Assert.Null(deletes);
                SortColumns(inserts.EventBatchData.Columns[0], inserts.EventBatchData.Columns[1]);
            }
            finally
            {
                inserts.Rent(1);
                inserts.Return();
            }
        }

        private static void SortColumns(IColumn col0, IColumn col1)
        {
            var sorter = new FlowtideDotNet.Core.ColumnStore.Sort.BatchSorter(2);
            int[] sorted = [0, 1];
            int[] tags = [0, 0];
            var sortedSpan = sorted.AsSpan();
            var tagSpan = tags.AsSpan();
            sorter.SortDataWithTags([col0, col1], ref sortedSpan, ref tagSpan);
            Assert.Equal(new int[] { 1, 0 }, sorted);
        }
    }
}
