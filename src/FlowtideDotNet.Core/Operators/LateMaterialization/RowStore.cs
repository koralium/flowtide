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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using System.Numerics;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    // Rows behind references, used by the thread of its host
    internal sealed class RowStore : IDisposable
    {
        private const int FormatVersion = 1;
        // A retired word is a sequence and 64 of its slots
        private const int WordBits = RowReference.SlotBits - 6;
        private const int FirstBufferSize = 1024;
        private const int WordBytes = 2 * sizeof(long);
        internal const int MaxRetireBufferSize = 1 << 20;

        private readonly string _name;
        private readonly RowStoreMode _mode;
        private readonly RowHeap _heap;
        private readonly IObjectState<RowStoreState> _state;
        // Never committed, a restart starts with both empty
        private readonly IBPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>[]? _retired;
        private readonly IBPlusTreeBulkInserter<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>[]? _inserters;
        private readonly int _bufferLimit;
        private ulong[] _buffer;
        private int _buffered;
        private long[] _wordKeys;
        private long[] _wordMasks;
        // Rows of a written word that were retired before
        private long _overlapKey;
        private ulong _overlap;
        // The tree that takes the rows retired now
        private int _current;
        private long _retiredNow;
        // Retired before the barrier, released when its checkpoint is committed
        private long _frozen;
        private bool _begun;
        private bool _stateIsNew;
        private bool _failed;
        private bool _disposed;

        private RowStore(
            string name,
            RowStoreMode mode,
            RowHeap heap,
            IObjectState<RowStoreState> state,
            bool stateIsNew,
            IBPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>[]? retired,
            int bufferSize)
        {
            _name = name;
            _mode = mode;
            _heap = heap;
            _state = state;
            _stateIsNew = stateIsNew;
            _retired = retired;
            _bufferLimit = bufferSize;
            _buffer = new ulong[mode == RowStoreMode.Keyed ? Math.Min(bufferSize, FirstBufferSize) : 0];
            _wordKeys = Array.Empty<long>();
            _wordMasks = Array.Empty<long>();
            if (retired != null)
            {
                _inserters = new IBPlusTreeBulkInserter<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>[retired.Length];
                for (int i = 0; i < retired.Length; i++)
                {
                    _inserters[i] = retired[i].CreateBulkInserter();
                }
            }
        }

        public RowStoreMode Mode => _mode;

        public long RowCount => _heap.RowCount;

        // Retired rows that are not released yet
        public long WaitingRows => _retiredNow + _frozen;

        internal RowHeap Heap => _heap;

        public static async Task<RowStore> CreateAsync(IStateManagerClient stateManagerClient, string name, int columnCount, RowStoreOptions options, IMemoryAllocator memoryAllocator)
        {
            if (options.Mode != RowStoreMode.Append && options.Mode != RowStoreMode.Keyed)
            {
                throw new ArgumentOutOfRangeException(nameof(options), $"A row store is opened in append or keyed mode, got {options.Mode}.");
            }
            if (options.RetireBufferSize < 1 || options.RetireBufferSize > MaxRetireBufferSize)
            {
                throw new ArgumentOutOfRangeException(nameof(options), $"The buffer of retired rows holds 1 to {MaxRetireBufferSize} rows, got {options.RetireBufferSize}.");
            }
            var state = await stateManagerClient.GetOrCreateObjectStateAsync<RowStoreState>($"{name}_store");
            var stored = state.Value;
            if (stored != null && (stored.FormatVersion != FormatVersion || stored.Mode != (int)options.Mode))
            {
                throw new InvalidOperationException(
                    $"Row store '{name}' is stored with format {stored.FormatVersion} in mode {(RowStoreMode)stored.Mode}, " +
                    $"the stream expects format {FormatVersion} in mode {options.Mode}.");
            }
            // Trees first, a failure here leaves no heap behind
            IBPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>[]? retired = null;
            if (options.Mode == RowStoreMode.Keyed)
            {
                retired = new IBPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>[2];
                for (int i = 0; i < retired.Length; i++)
                {
                    retired[i] = await stateManagerClient.GetOrCreateTree($"{name}_retired_{i}", new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>
                    {
                        Comparer = new PrimitiveListComparer<long>(),
                        KeySerializer = new PrimitiveListKeyContainerSerializer<long>(memoryAllocator),
                        ValueSerializer = new PrimitiveListValueContainerSerializer<long>(memoryAllocator),
                        UseByteBasedPageSizes = true,
                        MemoryAllocator = memoryAllocator
                    });
                }
            }
            var heap = await RowHeap.CreateAsync(stateManagerClient, name, columnCount, options.Heap, memoryAllocator);
            if (stored == null)
            {
                state.Value = new RowStoreState()
                {
                    FormatVersion = FormatVersion,
                    Mode = (int)options.Mode
                };
            }
            return new RowStore(name, options.Mode, heap, state, stored == null, retired, options.RetireBufferSize);
        }

        private void ThrowIfNotUsable()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_failed)
            {
                throw new InvalidOperationException($"Row store '{_name}' failed in an earlier call, it is opened anew from the last checkpoint.");
            }
        }

        // Stores the rows and adds one reference for each
        public ValueTask AppendAsync(EventBatchData payload, int start, int count, Column references)
        {
            ThrowIfNotUsable();
            return _heap.AppendAsync(payload, start, count, references);
        }

        // Rows with weights, a store has no row to take back
        public ValueTask AppendAsync(EventBatchData payload, FlowtideDotNet.Storage.DataStructures.PrimitiveList<int> weights, Column references)
        {
            ThrowIfNotUsable();
            if (weights.Count != payload.Count)
            {
                throw new ArgumentException($"The batch has {payload.Count} rows and {weights.Count} weights.", nameof(weights));
            }
            for (int i = 0; i < weights.Count; i++)
            {
                if (weights[i] < 0)
                {
                    throw new InvalidOperationException($"Row {i} of the batch has weight {weights[i]}, row store '{_name}' only takes rows that are added.");
                }
            }
            return _heap.AppendAsync(payload, 0, weights.Count, references);
        }

        // The row is read until its checkpoint is committed
        public ValueTask RetireAsync(ulong reference)
        {
            ThrowIfNotUsable();
            if (_mode != RowStoreMode.Keyed)
            {
                throw new InvalidOperationException($"Row store '{_name}' is in append mode, a row cannot be retired.");
            }
            if (!_heap.IsLive(reference))
            {
                throw new InvalidOperationException(
                    $"Row reference to slot {RowReference.Slot(reference)} of page {RowReference.Sequence(reference)} in row store '{_name}' points at no row, it cannot be retired.");
            }
            if (_buffered == _buffer.Length)
            {
                if (_buffered == _bufferLimit)
                {
                    return RetireAsync_Slow(reference);
                }
                Array.Resize(ref _buffer, (int)Math.Min(_bufferLimit, _buffer.Length * 2L));
            }
            _buffer[_buffered++] = reference;
            _retiredNow++;
            return ValueTask.CompletedTask;
        }

        private async ValueTask RetireAsync_Slow(ulong reference)
        {
            await FlushAsync();
            _buffer[_buffered++] = reference;
            _retiredNow++;
        }

        // Adds the rows of a word to the ones written before
        private readonly struct RetireMutator : IRowMutator<long, long>
        {
            private readonly RowStore _store;

            public RetireMutator(RowStore store)
            {
                _store = store;
            }

            public void GetSizePrefixSum(long[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
            {
                for (int i = 0; i < indices.Length; i++)
                {
                    sizes[i] += (i + 1) * WordBytes;
                }
            }

            public GenericWriteOperation Process(long key, bool exists, in long existingData, ref long incomingData, int sortedIndex)
            {
                if (exists)
                {
                    if ((existingData & incomingData) != 0 && _store._overlap == 0)
                    {
                        _store._overlapKey = key;
                        _store._overlap = (ulong)(existingData & incomingData);
                    }
                    incomingData |= existingData;
                }
                return GenericWriteOperation.Upsert;
            }
        }

        private static long WordKey(ulong reference)
        {
            return (long)(reference >> 6);
        }

        private InvalidOperationException RetiredTwice(long key, ulong mask)
        {
            int slot = (int)((key & ((1L << WordBits) - 1)) << 6) + BitOperations.TrailingZeroCount(mask);
            return new InvalidOperationException($"The row at slot {slot} of page {key >> WordBits} in row store '{_name}' was already retired.");
        }

        // Writes the retired rows as words, one pass over the tree
        private async ValueTask FlushAsync()
        {
            try
            {
                Array.Sort(_buffer, 0, _buffered);
                if (_wordKeys.Length < _buffered)
                {
                    _wordKeys = new long[_buffer.Length];
                    _wordMasks = new long[_buffer.Length];
                }
                int words = 0;
                int i = 0;
                while (i < _buffered)
                {
                    long key = WordKey(_buffer[i]);
                    ulong mask = 0;
                    while (i < _buffered && WordKey(_buffer[i]) == key)
                    {
                        ulong bit = 1UL << (int)(_buffer[i] & 63);
                        if ((mask & bit) != 0)
                        {
                            throw RetiredTwice(key, bit);
                        }
                        mask |= bit;
                        i++;
                    }
                    _wordKeys[words] = key;
                    _wordMasks[words] = (long)mask;
                    words++;
                }
                await _inserters![_current].ApplyBatch(_wordKeys, _wordMasks, words, new RetireMutator(this), words * WordBytes);
                if (_overlap != 0)
                {
                    throw RetiredTwice(_overlapKey, _overlap);
                }
                _buffered = 0;
            }
            catch
            {
                _failed = true;
                throw;
            }
        }

        // Merges small pages, and sparse ones in keyed mode
        public ValueTask<bool> CompactAsync()
        {
            ThrowIfNotUsable();
            return _heap.CompactAsync();
        }

        public RowHeapReader CreateReader()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _heap.CreateReader();
        }

        // Called in OnCheckpoint, before the barrier is forwarded
        public async Task BeginCheckpointAsync()
        {
            ThrowIfNotUsable();
            if (_begun)
            {
                throw new InvalidOperationException($"Row store '{_name}' has a checkpoint that is not committed yet.");
            }
            try
            {
                if (_retired != null)
                {
                    if (_buffered > 0)
                    {
                        await FlushAsync();
                    }
                    // What is retired from here on belongs to the next checkpoint
                    _current = 1 - _current;
                    _frozen = _retiredNow;
                    _retiredNow = 0;
                }
                _heap.BeginCheckpoint();
                _begun = true;
            }
            catch
            {
                _failed = true;
                throw;
            }
        }

        // Called when the sinks are done, before the checkpoint is saved
        public async Task CheckpointAsync()
        {
            ThrowIfNotUsable();
            if (!_begun)
            {
                throw new InvalidOperationException($"Row store '{_name}' commits a checkpoint that was not begun at a barrier.");
            }
            try
            {
                if (_retired != null && _frozen > 0)
                {
                    // Every reader is past the barrier, the rows go now
                    var frozen = _retired[1 - _current];
                    await ReleaseAsync(frozen);
                    await frozen.Clear();
                    _frozen = 0;
                }
                await _heap.CheckpointAsync();
                if (_stateIsNew)
                {
                    await _state.Commit();
                    _stateIsNew = false;
                }
                _begun = false;
            }
            catch
            {
                _failed = true;
                throw;
            }
        }

        private async Task ReleaseAsync(IBPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>> tree)
        {
            long released = 0;
            using var iterator = tree.CreateIterator();
            await iterator.SeekFirst();
            await foreach (var leaf in iterator)
            {
                leaf.EnterWriteLock();
                try
                {
                    foreach (var entry in leaf)
                    {
                        ulong mask = (ulong)entry.Value;
                        while (mask != 0)
                        {
                            int bit = BitOperations.TrailingZeroCount(mask);
                            _heap.Release(((ulong)entry.Key << 6) | (uint)bit);
                            mask &= mask - 1;
                            released++;
                        }
                    }
                }
                finally
                {
                    leaf.ExitWriteLock();
                }
            }
            if (released != _frozen)
            {
                throw new InvalidOperationException($"Row store '{_name}' had {_frozen} retired rows to release and found {released}.");
            }
        }

        // Called when the host and every reader are done
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;
            _heap.Dispose();
        }
    }
}
