using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Join.MergeJoin;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System.Buffers;
using System.Reflection;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinOwnershipTests
    {
        private sealed class CountingAllocator : IMemoryAllocator
        {
            public long OutstandingBytes;
            public IMemoryOwner<byte> Allocate(int size, int alignment) => GlobalMemoryManager.Instance.Allocate(size, alignment);
            public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment) => GlobalMemoryManager.Instance.Realloc(memory, size, alignment);
            public void RegisterAllocationToMetrics(int size) => OutstandingBytes += size;
            public void RegisterFreeToMetrics(int size) => OutstandingBytes -= size;
        }

        private sealed class ThrowingInserter : IBPlusTreeBulkInserter<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer>
        {
            public PrimitiveList<int>? ComposedOffsets;
            public int LeafHitCount => 0;
            public int[] SortAndGetIndices(ColumnRowReference[] keys, int keyLength) => throw new NotSupportedException();
            public ValueTask ApplyBatch<TMutator>(ColumnRowReference[] keys, MultiJoinWeight[] values, int keyLength, TMutator mutator, int totalBatchByteSize)
                where TMutator : IRowMutator<ColumnRowReference, MultiJoinWeight> => throw new NotSupportedException();

            public ValueTask ApplyBatch<TMutator>(ColumnRowReference[] keys, MultiJoinWeight[] values, int keyLength, int[] sortedIndices, int[] duplicateTags, TMutator mutator, int totalBatchByteSize)
                where TMutator : IRowMutator<ColumnRowReference, MultiJoinWeight>
            {
                ComposedOffsets = Assert.IsType<ColumnWithOffset>(keys[0].referenceBatch.Columns[0]).Offsets;
                return ValueTask.FromException(new IOException("Injected secondary storage failure"));
            }
        }

        [Fact]
        public async Task SecondaryInsertFailureReleasesComposedOffsetsWithoutFinalization()
        {
            var allocator = new CountingAllocator();
            using var column = Column.Create(allocator);
            column.Add(new Int64Value(11));
            column.Add(new Int64Value(22));
            using var originalOffsets = new PrimitiveList<int>(allocator);
            originalOffsets.Add(0);
            originalOffsets.Add(1);
            var batch = new EventBatchData(new IColumn[] { new ColumnWithOffset(column, originalOffsets) });
            using var changedRows = new PrimitiveList<int>(allocator);
            changedRows.Add(0);

            var inserter = new ThrowingInserter();
            var writeColumns = new IColumn[1];
            var writer = new MultiJoinTreeWriter
            {
                Tree = null!,
                Inserter = inserter,
                Comparer = new MergeJoinInsertComparer(new List<int> { 0 }, 1),
                Sorter = new BatchSorter(1),
                SortColumns = new IColumn[1],
                ColumnCount = 1
            };
            var secondary = new MultiJoinSecondaryIndex
            {
                Layout = new[] { 0 },
                Writer = writer,
                AllColumns = new List<int> { 0 },
                WriteColumns = writeColumns,
                WriteBatch = new EventBatchData(writeColumns)
            };
            var state = new MultiJoinInputState(null!, 0);
            typeof(MultiJoinInputState).GetField("_changeDeltas", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(state, new[] { 1 });
            var apply = typeof(MultiJoinInputState).GetMethod("ApplySecondary", BindingFlags.Instance | BindingFlags.NonPublic)!;
            long baseline = allocator.OutstandingBytes;
            try
            {
                await Assert.ThrowsAsync<IOException>(() => ((ValueTask<int>)apply.Invoke(state,
                    new object[] { secondary, batch, changedRows, 1, 2, allocator })!).AsTask());
                Assert.NotNull(inserter.ComposedOffsets);
                Assert.NotSame(originalOffsets, inserter.ComposedOffsets);
                state.ReleaseScratch();
                Assert.Equal(baseline, allocator.OutstandingBytes);
            }
            finally
            {
                // What a failed assertion would leave behind
                inserter.ComposedOffsets?.Dispose();
                Array.Clear(writeColumns);
                writer.Release(1);
            }
        }
    }
}
