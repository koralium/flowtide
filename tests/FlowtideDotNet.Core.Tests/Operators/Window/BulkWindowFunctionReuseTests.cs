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
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk;
using FlowtideDotNet.Core.Operators.Window.Bulk;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Core.Tests.Operators.Window
{
    /// <summary>
    /// The engine reinitializes the same function instance after a failure, so nothing may
    /// carry over from the previous generation.
    /// </summary>
    public class BulkWindowFunctionReuseTests : IAsyncLifetime
    {
        private StateManagerSync<object>? _stateManager;
        private IBPlusTree<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer>? _persistentTree;
        private IStateManagerClient? _client;

        public async Task InitializeAsync()
        {
            _stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions())
            }, NullLoggerFactory.Instance, new Meter("storage"), "storage", GlobalMemoryManager.Instance);
            await _stateManager.InitializeAsync();

            _client = _stateManager.GetOrCreateClient("window");
            _persistentTree = await _client.GetOrCreateTree("persistent_v1",
                new BPlusTreeOptions<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer>()
                {
                    Comparer = new BulkWindowInsertComparer(null, new List<int>() { 0 }, new List<int>()),
                    KeySerializer = new ColumnStoreSerializer(1, GlobalMemoryManager.Instance),
                    ValueSerializer = new BulkWindowValueContainerSerializer(1, GlobalMemoryManager.Instance),
                    MemoryAllocator = GlobalMemoryManager.Instance,
                    UseByteBasedPageSizes = true,
                    UsePreviousPointers = true
                });
        }

        public Task DisposeAsync()
        {
            _stateManager?.Dispose();
            return Task.CompletedTask;
        }

        private BulkWindowFunctionContext CreateContext()
        {
            return new BulkWindowFunctionContext()
            {
                PersistentTree = _persistentTree!,
                PartitionColumns = new List<int>() { 0 },
                OrderBy = new List<SortField>(),
                FunctionsRegister = new FunctionsRegister(),
                CreateInsertComparer = () => new BulkWindowInsertComparer(null, new List<int>() { 0 }, new List<int>()),
                FunctionIndex = 0,
                AuxiliaryColumnStartIndex = 1,
                MemoryAllocator = GlobalMemoryManager.Instance,
                StateManagerClient = _client!
            };
        }

        // One row whose only column is the partition value
        private static ColumnRowReference PartitionRow(string partition)
        {
            var column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new StringValue(partition));
            return new ColumnRowReference()
            {
                referenceBatch = new EventBatchData(new Column[] { column }),
                RowIndex = 0
            };
        }

        /// <summary>
        /// Leaves the function with an entry loaded and no row found, the state that made a
        /// second generation read pooled columns.
        /// </summary>
        private static async Task LoadEntryWithoutComputing(
            BulkSurrogateKeyInt64WindowFunction function,
            BulkWindowSeedReader seedReader,
            ColumnRowReference partition)
        {
            var result = new DataValueContainer();

            // First scan creates the key for the partition.
            await function.StartScan(partition, seedReader, true);
            await function.ComputeRow(new BulkWindowRowContext(1) { Batch = partition.referenceBatch, RowIndex = 0 }, result);
            await function.EndScan();

            // Second scan finds the stored key and computes nothing.
            await function.StartScan(partition, seedReader, true);
        }

        [Fact]
        public async Task SurrogateKeyDoesNotCarryScanStateIntoTheNextGeneration()
        {
            var partition = PartitionRow("p1");
            using var backwardReader = new BulkWindowBackwardPartitionReader(
                _persistentTree!,
                new BulkWindowInsertComparer(null, new List<int>() { 0 }, new List<int>()),
                new List<int>() { 0 });
            using var seedReader = new BulkWindowSeedReader(backwardReader, 1, 1, GlobalMemoryManager.Instance);
            seedReader.ResetEmpty();

            var function = new BulkSurrogateKeyInt64WindowFunction();
            await function.Initialize(CreateContext());
            await LoadEntryWithoutComputing(function, seedReader, partition);

            long firstKey;
            var probe = new DataValueContainer();
            await function.StartScan(partition, seedReader, true);
            await function.ComputeRow(new BulkWindowRowContext(1) { Batch = partition.referenceBatch, RowIndex = 0 }, probe);
            firstKey = probe.AsLong;
            await function.EndScan();

            // Put the instance back into the loaded but uncomputed state, then fail.
            await LoadEntryWithoutComputing(function, seedReader, partition);
            function.Dispose();

            // The engine reuses this instance, and EndScan can run before any StartScan.
            await function.Initialize(CreateContext());
            await function.EndScan();

            // The partition still holds rows, so its key must have survived.
            var afterRestart = new DataValueContainer();
            await function.StartScan(partition, seedReader, true);
            await function.ComputeRow(new BulkWindowRowContext(1) { Batch = partition.referenceBatch, RowIndex = 0 }, afterRestart);
            await function.EndScan();

            Assert.Equal(firstKey, afterRestart.AsLong);
            function.Dispose();
        }
    }
}
