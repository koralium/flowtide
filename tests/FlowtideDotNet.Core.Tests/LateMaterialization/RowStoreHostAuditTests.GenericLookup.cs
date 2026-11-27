using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Core.Sources.Generic.Internal;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;
using System.Reflection;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public partial class RowStoreHostAuditTests
{
    private sealed class GatedGenericKeyColumn : Column, IColumn
    {
        private readonly TaskCompletionSource _updated;
        private readonly ManualResetEventSlim _release;
        private int _calls;

        public GatedGenericKeyColumn(TaskCompletionSource updated, ManualResetEventSlim release)
            : base(GlobalMemoryManager.Instance)
        {
            _updated = updated;
            _release = release;
            Add(NullValue.Instance);
        }

        void IColumn.UpdateAt<T>(in int index, in T value)
        {
            base.UpdateAt(index, value);
            if (Interlocked.Increment(ref _calls) == 1)
            {
                _updated.TrySetResult();
                if (!_release.Wait(TimeSpan.FromSeconds(30)))
                {
                    throw new TimeoutException("Generic key update was not released.");
                }
            }
        }
    }

    [Fact]
    public async Task ParallelGenericLookupsDoNotOverwriteKeyBeforeHostCopiesIt()
    {
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        using var meter = new Meter("generic-parallel-copy");
        using var manager = new StateManagerSync<StreamState>(new StateManagerOptions { PersistentStorage = storage },
            NullLoggerFactory.Instance, meter, "generic-parallel-copy", GlobalMemoryManager.Instance);
        await manager.InitializeAsync();
        var op = new InspectableGenericReadOperator(new GatedGenericDataSource([], []), GenericRelation());
        await InitializeDirect(op, new ImmediateReturnTarget(), manager, meter);
        try
        {
            var flags = BindingFlags.Instance | BindingFlags.NonPublic;
            var host = (RowStoreHost)typeof(ColumnBatchReadBaseOperator).GetField("_host", flags)!.GetValue(op)!;
            using var input = new EventBatchData([Column.Create(GlobalMemoryManager.Instance), Column.Create(GlobalMemoryManager.Instance), Column.Create(GlobalMemoryManager.Instance)]);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 1, 1 };
            for (int key = 1; key <= 2; key++)
            {
                input.Columns[0].Add(new Int64Value(key));
                input.Columns[1].Add(new StringValue("payload-" + key));
                input.Columns[2].Add(new StringValue("k" + key));
            }
            using (await host.ApplyAsync(input, weights, null)) { }
            var columns = (IColumn[])typeof(GenericReadOperator<GenericItem>).GetField("_keyLookupColumns", flags)!.GetValue(op)!;
            var updated = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var release = new ManualResetEventSlim();
            columns[2].Dispose();
            using var gated = new GatedGenericKeyColumn(updated, release);
            columns[2] = gated;
            GenericItem? firstItem = null;
            GenericItem? secondItem = null;
            var first = Task.Run(async () => await op.LookupPublic("k1"));
            try
            {
                await updated.Task.WaitAsync(TimeSpan.FromSeconds(30));
                secondItem = await op.LookupPublic("k2");
            }
            finally
            {
                release.Set();
                firstItem = await first.WaitAsync(TimeSpan.FromSeconds(30));
            }
            Assert.Equal(2, secondItem?.Id);
            Assert.Equal(1, firstItem?.Id);
            Assert.Equal("payload-1", firstItem?.Text);
        }
        finally
        {
            await op.DisposeAsync();
        }
    }
}
