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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.SurrogateKey;
using FlowtideDotNet.Core.Operators.Window.Bulk;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk
{
    internal class BulkSurrogateKeyInt64WindowFunctionDefinition : BulkWindowFunctionDefinition
    {
        public override bool TryCreate(WindowFunction windowFunction, IFunctionsRegister functionsRegister, [NotNullWhen(true)] out IBulkWindowFunction? bulkWindowFunction)
        {
            bulkWindowFunction = new BulkSurrogateKeyInt64WindowFunction();
            return true;
        }
    }

    /// <summary>
    /// Assigns a stable int64 key per partition, stored in its own tree so the key survives across
    /// changes to the partition's rows. The entry is created when the partition first produces a row and
    /// removed when a scan finds the partition empty.
    /// </summary>
    internal class BulkSurrogateKeyInt64WindowFunction : IBulkWindowFunction
    {
        private List<int>? _partitionColumns;
        private IBPlusTree<ColumnRowReference, SurrogateKeyValue, ColumnKeyStorageContainer, SurrogateKeyValueContainer>? _tree;
        private IObjectState<long>? _keyCounterState;
        private BulkWindowForwardPartitionReader? _emptyCheckReader;

        // Scratch batch holding only the partition columns, which is the surrogate tree's key layout.
        private Column[]? _partitionKeyColumns;
        private EventBatchData? _partitionKeyBatch;
        // Full width scratch row used by the empty check reader, which expects full row keys.
        private Column[]? _fullRowColumns;
        private EventBatchData? _fullRowBatch;
        private IMemoryAllocator? _memoryAllocator;
        private Column? _currentValueColumn;
        private bool _entryLoaded;
        private bool _rowFound;

        public long AffectedRowsBefore => 0;

        public long AffectedRowsAfter => 0;

        public bool StableByValueEquality => false;

        public long EqualityStableAfterRows => long.MaxValue;

        public int AuxiliaryStateColumnCount => 0;

        public async Task Initialize(BulkWindowFunctionContext context)
        {
            _partitionColumns = context.PartitionColumns;
            _memoryAllocator = context.MemoryAllocator;
            _tree = await context.StateManagerClient.GetOrCreateTree("partitions",
                new BPlusTreeOptions<ColumnRowReference, SurrogateKeyValue, ColumnKeyStorageContainer, SurrogateKeyValueContainer>()
                {
                    Comparer = new ColumnComparer(context.PartitionColumns.Count),
                    KeySerializer = new ColumnStoreSerializer(context.PartitionColumns.Count, context.MemoryAllocator),
                    ValueSerializer = new SurrogateKeyValueContainerSerializer(context.MemoryAllocator),
                    MemoryAllocator = context.MemoryAllocator
                });
            _keyCounterState = await context.StateManagerClient.GetOrCreateObjectStateAsync<long>("key_counter");
            _emptyCheckReader = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns);

            _partitionKeyColumns = new Column[context.PartitionColumns.Count];
            for (int i = 0; i < _partitionKeyColumns.Length; i++)
            {
                _partitionKeyColumns[i] = Column.Create(context.MemoryAllocator);
                _partitionKeyColumns[i].Add(NullValue.Instance);
            }
            _partitionKeyBatch = new EventBatchData(_partitionKeyColumns);
            _currentValueColumn = Column.Create(context.MemoryAllocator);
            _currentValueColumn.Add(NullValue.Instance);
        }

        public async ValueTask Commit()
        {
            Debug.Assert(_tree != null);
            Debug.Assert(_keyCounterState != null);
            await _tree.Commit();
            await _keyCounterState.Commit();
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_partitionColumns != null);
            Debug.Assert(_partitionKeyColumns != null);
            Debug.Assert(_tree != null);
            Debug.Assert(_currentValueColumn != null);

            for (int i = 0; i < _partitionColumns.Count; i++)
            {
                _partitionKeyColumns[i].UpdateAt(0, partitionValues.referenceBatch.Columns[_partitionColumns[i]].GetValueAt(partitionValues.RowIndex, default));
            }

            if (_fullRowColumns == null)
            {
                _fullRowColumns = new Column[partitionValues.referenceBatch.Columns.Count];
                for (int i = 0; i < _fullRowColumns.Length; i++)
                {
                    _fullRowColumns[i] = Column.Create(_memoryAllocator!);
                    _fullRowColumns[i].Add(NullValue.Instance);
                }
                _fullRowBatch = new EventBatchData(_fullRowColumns);
            }
            for (int i = 0; i < _fullRowColumns.Length; i++)
            {
                _fullRowColumns[i].UpdateAt(0, partitionValues.referenceBatch.Columns[i].GetValueAt(partitionValues.RowIndex, default));
            }

            _rowFound = false;
            var partitionKey = new ColumnRowReference()
            {
                referenceBatch = _partitionKeyBatch!,
                RowIndex = 0
            };
            var result = await _tree.GetValue(in partitionKey);
            if (result.found)
            {
                _currentValueColumn.UpdateAt(0, result.value.Value);
                _entryLoaded = true;
            }
            else
            {
                // Created lazily on the first computed row so empty partitions do not allocate keys.
                _entryLoaded = false;
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            if (!_entryLoaded)
            {
                // Creating the entry writes to the tree, which is asynchronous.
                return false;
            }
            _rowFound = true;
            _currentValueColumn!.GetValueAt(0, result, default);
            return true;
        }

        public async ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_tree != null);
            Debug.Assert(_keyCounterState != null);
            Debug.Assert(_currentValueColumn != null);

            if (!_entryLoaded)
            {
                var newValue = new Int64Value(_keyCounterState.Value++);
                _currentValueColumn.UpdateAt(0, newValue);
                await _tree.Upsert(new ColumnRowReference() { referenceBatch = _partitionKeyBatch!, RowIndex = 0 }, new SurrogateKeyValue()
                {
                    Value = newValue,
                    Weight = 1
                });
                _entryLoaded = true;
            }
            _rowFound = true;
            _currentValueColumn.GetValueAt(0, result, default);
        }

        public async ValueTask EndScan()
        {
            Debug.Assert(_tree != null);
            Debug.Assert(_emptyCheckReader != null);

            if (_rowFound || !_entryLoaded)
            {
                return;
            }

            // The scan produced no rows but an entry exists. The scan may have covered only part of the
            // partition, so verify the partition is empty before removing the surrogate key.
            var fullRowKey = new ColumnRowReference()
            {
                referenceBatch = _fullRowBatch!,
                RowIndex = 0
            };
            await _emptyCheckReader.Reset(fullRowKey);
            if (!await _emptyCheckReader.MoveNextRow())
            {
                await _tree.Delete(new ColumnRowReference()
                {
                    referenceBatch = _partitionKeyBatch!,
                    RowIndex = 0
                });
                _entryLoaded = false;
            }
        }
    }
}
