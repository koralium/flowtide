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

using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.Tests
{
    public class BPlusTreeBulkInserterTests : IDisposable
    {
        private readonly BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>> _tree;
        private readonly StateManagerSync _stateManager;

        public BPlusTreeBulkInserterTests()
        {
            (_tree, _stateManager) = Init().GetAwaiter().GetResult();
        }

        private static async Task<(BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>, StateManagerSync)> Init()
        {
            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions())
            }, NullLoggerFactory.Instance, new Meter("bulk_inserter_test"), "bulk_inserter_test");
            await stateManager.InitializeAsync();
            
            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = await nodeClient.GetOrCreateTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>("tree",
                new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>()
                {
                    PageSizeBytes = 100,
                    UseByteBasedPageSizes = true,
                    Comparer = new PrimitiveListComparer<long>(),
                    KeySerializer = new PrimitiveListKeyContainerSerializer<long>(GlobalMemoryManager.Instance),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<long>(GlobalMemoryManager.Instance),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });

            return ((BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>)tree, stateManager);
        }

        #region Mutators

        /// <summary>
        /// Always upserts: inserts new keys, updates existing ones.
        /// </summary>
        private struct UpsertMutator : IRowMutator<long, long>
        {
            public GenericWriteOperation Process(long key, bool exists, in long existingData, ref long incomingData)
            {
                return GenericWriteOperation.Upsert;
            }
        }

        /// <summary>
        /// Always deletes if the key exists, otherwise does nothing.
        /// </summary>
        private struct DeleteIfExistsMutator : IRowMutator<long, long>
        {
            public GenericWriteOperation Process(long key, bool exists, in long existingData, ref long incomingData)
            {
                return exists ? GenericWriteOperation.Delete : GenericWriteOperation.None;
            }
        }

        /// <summary>
        /// Upserts if the key does not exist, deletes if it does exist.
        /// Useful for toggle-like behavior.
        /// </summary>
        private struct ToggleMutator : IRowMutator<long, long>
        {
            public GenericWriteOperation Process(long key, bool exists, in long existingData, ref long incomingData)
            {
                return exists ? GenericWriteOperation.Delete : GenericWriteOperation.Upsert;
            }
        }

        /// <summary>
        /// Conditionally upserts: only inserts/updates if incomingData > existingData (or key is new).
        /// </summary>
        private struct ConditionalUpsertMutator : IRowMutator<long, long>
        {
            public GenericWriteOperation Process(long key, bool exists, in long existingData, ref long incomingData)
            {
                if (!exists || incomingData > existingData)
                {
                    return GenericWriteOperation.Upsert;
                }
                return GenericWriteOperation.None;
            }
        }

        /// <summary>
        /// No-op mutator: never modifies the tree.
        /// </summary>
        private struct NoOpMutator : IRowMutator<long, long>
        {
            public GenericWriteOperation Process(long key, bool exists, in long existingData, ref long incomingData)
            {
                return GenericWriteOperation.None;
            }
        }

        /// <summary>
        /// Stateful mutator: deletes a key on first encounter, re-inserts on second.
        /// Uses a class wrapper to maintain state across calls within a single batch.
        /// </summary>
        private class DeleteThenReinsertMutator : IRowMutator<long, long>
        {
            private readonly HashSet<long> _deletedInThisBatch = new();

            public GenericWriteOperation Process(long key, bool exists, in long existingData, ref long incomingData)
            {
                if (exists && !_deletedInThisBatch.Contains(key))
                {
                    // First encounter: delete the existing key
                    _deletedInThisBatch.Add(key);
                    return GenericWriteOperation.Delete;
                }
                // Second encounter (or key didn't exist): re-insert
                return GenericWriteOperation.Upsert;
            }
        }

        #endregion

        #region Helpers

        private BPlusTreeBulkInserter<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>> CreateBulkInserter()
        {
            return new BPlusTreeBulkInserter<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>(_tree);
        }

        /// <summary>
        /// Iterates the entire tree and returns keys and values in sorted order.
        /// </summary>
        private async Task<List<(long key, long value)>> ReadAllFromTree()
        {
            var result = new List<(long key, long value)>();
            var it = _tree.CreateIterator();
            await it.SeekFirst();
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    result.Add((kv.Key, kv.Value));
                }
            }
            return result;
        }

        /// <summary>
        /// Asserts that the tree contains exactly the given key-value pairs in sorted key order.
        /// </summary>
        private async Task AssertTreeContents(SortedDictionary<long, long> expected)
        {
            var actual = await ReadAllFromTree();
            var expectedList = expected.ToList();

            Assert.Equal(expectedList.Count, actual.Count);
            for (int i = 0; i < expectedList.Count; i++)
            {
                Assert.Equal(expectedList[i].Key, actual[i].key);
                Assert.Equal(expectedList[i].Value, actual[i].value);
            }
        }

        /// <summary>
        /// Verifies each key individually via GetValue.
        /// </summary>
        private async Task AssertGetValueForAll(SortedDictionary<long, long> expected)
        {
            foreach (var kv in expected)
            {
                var (found, value) = await _tree.GetValue(kv.Key);
                Assert.True(found, $"Key {kv.Key} not found in tree");
                Assert.Equal(kv.Value, value);
            }
        }

        /// <summary>
        /// Generates an array of unique random keys.
        /// </summary>
        private long[] GenerateUniqueRandomKeys(Random rng, int count, int maxValue)
        {
            var set = new HashSet<long>();
            while (set.Count < count)
            {
                set.Add(rng.Next(0, maxValue));
            }
            return set.ToArray();
        }

        #endregion

        #region Insert Tests

        [Fact]
        public async Task InsertIntoEmptyTree_AscendingOrder()
        {
            var bulkInserter = CreateBulkInserter();
            var count = 50;
            var keys = new long[count];
            var values = new long[count];

            for (int i = 0; i < count; i++)
            {
                keys[i] = i;
                values[i] = i * 10;
            }

            await bulkInserter.ApplyBatch(keys, values, count, new UpsertMutator());

            var expected = new SortedDictionary<long, long>();
            for (int i = 0; i < count; i++)
                expected[i] = i * 10;

            await AssertTreeContents(expected);
            await AssertGetValueForAll(expected);
        }

        [Fact]
        public async Task InsertIntoEmptyTree_DescendingOrder()
        {
            var bulkInserter = CreateBulkInserter();
            var count = 50;
            var keys = new long[count];
            var values = new long[count];

            for (int i = 0; i < count; i++)
            {
                keys[i] = count - 1 - i; // Descending order
                values[i] = (count - 1 - i) * 10;
            }

            await bulkInserter.ApplyBatch(keys, values, count, new UpsertMutator());

            var expected = new SortedDictionary<long, long>();
            for (int i = 0; i < count; i++)
                expected[i] = i * 10;

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task InsertIntoEmptyTree_RandomOrder_UniqueKeys()
        {
            var bulkInserter = CreateBulkInserter();
            var rng = new Random(42);
            var count = 100;
            var keys = GenerateUniqueRandomKeys(rng, count, 10000);
            var values = new long[count];
            var expected = new SortedDictionary<long, long>();

            for (int i = 0; i < count; i++)
            {
                values[i] = keys[i] * 10;
                expected[keys[i]] = keys[i] * 10;
            }

            await bulkInserter.ApplyBatch(keys, values, count, new UpsertMutator());

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task InsertSingleElement()
        {
            var bulkInserter = CreateBulkInserter();
            var keys = new long[] { 42 };
            var values = new long[] { 420 };

            await bulkInserter.ApplyBatch(keys, values, 1, new UpsertMutator());

            var (found, value) = await _tree.GetValue(42);
            Assert.True(found);
            Assert.Equal(420, value);

            var all = await ReadAllFromTree();
            Assert.Single(all);
        }

        [Fact]
        public async Task InsertWithPartialArrayLength()
        {
            var bulkInserter = CreateBulkInserter();
            var keys = new long[100];
            var values = new long[100];

            // Fill all 100, but only use the first 10
            for (int i = 0; i < 100; i++)
            {
                keys[i] = i;
                values[i] = i * 10;
            }

            await bulkInserter.ApplyBatch(keys, values, 10, new UpsertMutator());

            var expected = new SortedDictionary<long, long>();
            for (int i = 0; i < 10; i++)
                expected[i] = i * 10;

            await AssertTreeContents(expected);

            // Keys 10-99 should NOT be in the tree
            for (int i = 10; i < 100; i++)
            {
                var (found, _) = await _tree.GetValue(i);
                Assert.False(found, $"Key {i} should not be in tree (beyond keyLength)");
            }
        }

        [Fact]
        public async Task DuplicateKeysInBatch_ShouldHaveOneEntryPerUniqueKey()
        {
            // BUG: When duplicate keys appear in the same batch, the bulk inserter
            // inserts the key multiple times because FindIndex runs against the
            // original leaf (inserts are deferred to InsertFrom at the end).
            // The second occurrence doesn't see the pending first insert, so it
            // also queues an insert — resulting in duplicate entries in the tree.
            //
            // Expected behavior: only one entry per unique key, last value wins.

            var bulkInserter = CreateBulkInserter();

            var keys = new long[] { 5, 5, 5 };
            var values = new long[] { 100, 200, 300 };

            await bulkInserter.ApplyBatch(keys, values, 3, new UpsertMutator());

            // There should be exactly 1 entry for key 5
            var all = await ReadAllFromTree();
            var entriesForKey5 = all.Where(x => x.key == 5).ToList();
            Assert.Single(entriesForKey5);

            // Total tree size should be 1, not 3
            Assert.Single(all);
        }

        [Fact]
        public async Task DuplicateKeysInBatch_LastValueShouldWin()
        {
            // When duplicate keys are in a batch, the last occurrence's value
            // should be the one stored (standard upsert semantics).

            var bulkInserter = CreateBulkInserter();

            var keys = new long[] { 1, 2, 1, 2, 1 };
            var values = new long[] { 10, 20, 100, 200, 1000 };

            await bulkInserter.ApplyBatch(keys, values, 5, new UpsertMutator());

            var all = await ReadAllFromTree();

            // Should have exactly 2 unique keys
            Assert.Equal(2, all.Count);

            // Key 1: last occurrence has value 1000
            var (found1, val1) = await _tree.GetValue(1);
            Assert.True(found1);
            Assert.Equal(1000, val1);

            // Key 2: last occurrence has value 200
            var (found2, val2) = await _tree.GetValue(2);
            Assert.True(found2);
            Assert.Equal(200, val2);
        }

        [Fact]
        public async Task DuplicateKeysInBatch_MixedWithUniqueKeys()
        {
            // Mix of unique and duplicate keys in a single batch.

            var bulkInserter = CreateBulkInserter();

            var keys = new long[] { 1, 2, 3, 2, 4, 3 };
            var values = new long[] { 10, 20, 30, 200, 40, 300 };

            await bulkInserter.ApplyBatch(keys, values, 6, new UpsertMutator());

            var all = await ReadAllFromTree();

            // Should have exactly 4 unique keys: 1, 2, 3, 4
            Assert.Equal(4, all.Count);

            var expected = new SortedDictionary<long, long>
            {
                { 1, 10 },
                { 2, 200 },  // last value for key 2
                { 3, 300 },  // last value for key 3
                { 4, 40 },
            };

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task DuplicateKeysInBatch_WithPreExistingData()
        {
            // When duplicates appear in a batch and the key already exists in the tree.

            var bulkInserter = CreateBulkInserter();

            // Pre-populate key 5
            var setupKeys = new long[] { 5 };
            var setupValues = new long[] { 50 };
            await bulkInserter.ApplyBatch(setupKeys, setupValues, 1, new UpsertMutator());

            // Batch with key 5 appearing twice
            var keys = new long[] { 5, 5 };
            var values = new long[] { 500, 5000 };
            await bulkInserter.ApplyBatch(keys, values, 2, new UpsertMutator());

            var all = await ReadAllFromTree();
            Assert.Single(all);

            var (found, val) = await _tree.GetValue(5);
            Assert.True(found);
            Assert.Equal(5000, val); // Last value should win
        }

        [Fact]
        public async Task RandomBatchWithDuplicates_TreeCountMatchesUniqueKeys()
        {
            // Generates random keys (with inevitable duplicates) and verifies
            // the tree has exactly one entry per unique key.

            var bulkInserter = CreateBulkInserter();
            var rng = new Random(42);
            var count = 100;
            var keys = new long[count];
            var values = new long[count];
            var expected = new SortedDictionary<long, long>();

            for (int i = 0; i < count; i++)
            {
                keys[i] = rng.Next(0, 50); // Small range → many duplicates
                values[i] = i;
                expected[keys[i]] = i; // SortedDictionary keeps last value per key
            }

            await bulkInserter.ApplyBatch(keys, values, count, new UpsertMutator());

            var all = await ReadAllFromTree();
            Assert.Equal(expected.Count, all.Count);
        }
        [Fact]
        public async Task DuplicateKeysInBatchPreserveOrder()
        {
            // When duplicates appear in a batch, they should be processed in the original sequence
            // to ensure standard upsert semantics where the chronologically last occurrence wins.
            
            var bulkInserter = CreateBulkInserter();
            var count = 10000;
            var keys = new long[count];
            var values = new long[count];
            
            int targetKeyCount = 0;
            var rng = new Random(1234);

            for (int i = 0; i < count; i++)
            {
                if (i % 2 == 0)
                {
                    keys[i] = 42;
                    values[i] = targetKeyCount++;
                }
                else
                {
                    do
                    {
                        keys[i] = rng.Next(0, 100);
                    } while (keys[i] == 42);

                    values[i] = -1;
                }
            }

            await bulkInserter.ApplyBatch(keys, values, count, new UpsertMutator());

            var (found, val) = await _tree.GetValue(42);
            Assert.True(found);
            Assert.Equal(targetKeyCount - 1, val); 
        }

        #endregion

        #region Update Tests

        [Fact]
        public async Task UpdateExistingKeys()
        {
            var bulkInserter = CreateBulkInserter();

            // First batch: insert keys 0-9
            var keys = new long[10];
            var values = new long[10];
            for (int i = 0; i < 10; i++)
            {
                keys[i] = i;
                values[i] = i * 10;
            }
            await bulkInserter.ApplyBatch(keys, values, 10, new UpsertMutator());

            // Second batch: update keys 0-9 with new values
            for (int i = 0; i < 10; i++)
            {
                values[i] = i * 100; // Updated values
            }
            await bulkInserter.ApplyBatch(keys, values, 10, new UpsertMutator());

            var expected = new SortedDictionary<long, long>();
            for (int i = 0; i < 10; i++)
                expected[i] = i * 100;

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task ConditionalUpdate_OnlyIfGreater()
        {
            var bulkInserter = CreateBulkInserter();

            // Insert initial values
            var keys = new long[] { 1, 2, 3 };
            var values = new long[] { 100, 200, 300 };
            await bulkInserter.ApplyBatch(keys, values, 3, new UpsertMutator());

            // Conditionally update: only key=2 should be updated (500 > 200), key=1 should not (50 < 100)
            var keys2 = new long[] { 1, 2 };
            var values2 = new long[] { 50, 500 };
            await bulkInserter.ApplyBatch(keys2, values2, 2, new ConditionalUpsertMutator());

            var (found1, val1) = await _tree.GetValue(1);
            Assert.True(found1);
            Assert.Equal(100, val1); // Unchanged

            var (found2, val2) = await _tree.GetValue(2);
            Assert.True(found2);
            Assert.Equal(500, val2); // Updated

            var (found3, val3) = await _tree.GetValue(3);
            Assert.True(found3);
            Assert.Equal(300, val3); // Untouched
        }

        #endregion

        #region Delete Tests

        [Fact]
        public async Task DeleteExistingKeys()
        {
            var bulkInserter = CreateBulkInserter();

            // Insert keys 0-19
            var keys = new long[20];
            var values = new long[20];
            for (int i = 0; i < 20; i++)
            {
                keys[i] = i;
                values[i] = i * 10;
            }
            await bulkInserter.ApplyBatch(keys, values, 20, new UpsertMutator());

            // Delete even keys
            var deleteKeys = new long[10];
            var deleteValues = new long[10];
            for (int i = 0; i < 10; i++)
            {
                deleteKeys[i] = i * 2;
                deleteValues[i] = 0;
            }
            await bulkInserter.ApplyBatch(deleteKeys, deleteValues, 10, new DeleteIfExistsMutator());

            // Only odd keys should remain
            var expected = new SortedDictionary<long, long>();
            for (int i = 0; i < 20; i++)
            {
                if (i % 2 != 0)
                    expected[i] = i * 10;
            }

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task DeleteAllKeys_TreeBecomesEmpty()
        {
            var bulkInserter = CreateBulkInserter();

            // Insert keys 0-9
            var count = 10;
            var keys = new long[count];
            var values = new long[count];
            for (int i = 0; i < count; i++)
            {
                keys[i] = i;
                values[i] = i;
            }
            await bulkInserter.ApplyBatch(keys, values, count, new UpsertMutator());

            // Delete all keys
            await bulkInserter.ApplyBatch(keys, values, count, new DeleteIfExistsMutator());

            var all = await ReadAllFromTree();
            Assert.Empty(all);
        }

        [Fact]
        public async Task DeleteSingleKey()
        {
            var bulkInserter = CreateBulkInserter();

            var keys = new long[] { 1, 2, 3 };
            var values = new long[] { 10, 20, 30 };
            await bulkInserter.ApplyBatch(keys, values, 3, new UpsertMutator());

            // Delete just the middle key
            var deleteKeys = new long[] { 2 };
            var deleteValues = new long[] { 0 };
            await bulkInserter.ApplyBatch(deleteKeys, deleteValues, 1, new DeleteIfExistsMutator());

            var expected = new SortedDictionary<long, long>
            {
                { 1, 10 },
                { 3, 30 },
            };

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task DeleteFirstAndLastKeys()
        {
            var bulkInserter = CreateBulkInserter();

            var keys = new long[] { 1, 2, 3, 4, 5 };
            var values = new long[] { 10, 20, 30, 40, 50 };
            await bulkInserter.ApplyBatch(keys, values, 5, new UpsertMutator());

            // Delete first and last
            var deleteKeys = new long[] { 1, 5 };
            var deleteValues = new long[] { 0, 0 };
            await bulkInserter.ApplyBatch(deleteKeys, deleteValues, 2, new DeleteIfExistsMutator());

            var expected = new SortedDictionary<long, long>
            {
                { 2, 20 },
                { 3, 30 },
                { 4, 40 },
            };

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task DeleteNonExistentKeys_TreeUnchanged()
        {
            var bulkInserter = CreateBulkInserter();

            // Insert keys 0-4
            var keys = new long[5];
            var values = new long[5];
            for (int i = 0; i < 5; i++)
            {
                keys[i] = i;
                values[i] = i * 10;
            }
            await bulkInserter.ApplyBatch(keys, values, 5, new UpsertMutator());

            // Try to delete keys that don't exist
            var deleteKeys = new long[] { 100, 200, 300 };
            var deleteValues = new long[] { 0, 0, 0 };
            await bulkInserter.ApplyBatch(deleteKeys, deleteValues, 3, new DeleteIfExistsMutator());

            // Original keys should all still be present
            var expected = new SortedDictionary<long, long>();
            for (int i = 0; i < 5; i++)
                expected[i] = i * 10;

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task DeleteMixOfExistentAndNonExistent()
        {
            var bulkInserter = CreateBulkInserter();

            var keys = new long[] { 1, 2, 3, 4, 5 };
            var values = new long[] { 10, 20, 30, 40, 50 };
            await bulkInserter.ApplyBatch(keys, values, 5, new UpsertMutator());

            // Delete keys 2, 4 (exist) and 7, 9 (don't exist)
            var deleteKeys = new long[] { 2, 4, 7, 9 };
            var deleteValues = new long[] { 0, 0, 0, 0 };
            await bulkInserter.ApplyBatch(deleteKeys, deleteValues, 4, new DeleteIfExistsMutator());

            var expected = new SortedDictionary<long, long>
            {
                { 1, 10 },
                { 3, 30 },
                { 5, 50 },
            };

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task ToggleMutator_InsertsAndDeletesInSameBatch()
        {
            var bulkInserter = CreateBulkInserter();

            // Pre-populate with keys 1, 3, 5
            var setupKeys = new long[] { 1, 3, 5 };
            var setupValues = new long[] { 10, 30, 50 };
            await bulkInserter.ApplyBatch(setupKeys, setupValues, 3, new UpsertMutator());

            // Toggle: keys 1,3,5 exist (will be deleted), keys 2,4 don't exist (will be inserted)
            var keys = new long[] { 1, 2, 3, 4, 5 };
            var values = new long[] { 0, 20, 0, 40, 0 };
            await bulkInserter.ApplyBatch(keys, values, 5, new ToggleMutator());

            var expected = new SortedDictionary<long, long>
            {
                { 2, 20 },
                { 4, 40 },
            };

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task AlternatingInsertAndDeleteBatches()
        {
            var bulkInserter = CreateBulkInserter();

            // Insert 0-19
            var keys = new long[20];
            var values = new long[20];
            for (int i = 0; i < 20; i++)
            {
                keys[i] = i;
                values[i] = i;
            }
            await bulkInserter.ApplyBatch(keys, values, 20, new UpsertMutator());

            // Delete 0-9
            var delKeys = new long[10];
            var delValues = new long[10];
            for (int i = 0; i < 10; i++)
            {
                delKeys[i] = i;
                delValues[i] = 0;
            }
            await bulkInserter.ApplyBatch(delKeys, delValues, 10, new DeleteIfExistsMutator());

            // Insert 20-29
            var keys2 = new long[10];
            var values2 = new long[10];
            for (int i = 0; i < 10; i++)
            {
                keys2[i] = 20 + i;
                values2[i] = 20 + i;
            }
            await bulkInserter.ApplyBatch(keys2, values2, 10, new UpsertMutator());

            var expected = new SortedDictionary<long, long>();
            for (int i = 10; i < 30; i++)
                expected[i] = i;

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task StressTest_InsertThenDeleteAll_InRandomBatches()
        {
            var bulkInserter = CreateBulkInserter();
            var count = 200;
            var keys = new long[count];
            var values = new long[count];

            for (int i = 0; i < count; i++)
            {
                keys[i] = i;
                values[i] = i;
            }

            // Insert all
            await bulkInserter.ApplyBatch(keys, values, count, new UpsertMutator());

            // Delete in random order batches
            var rng = new Random(111);
            var remainingKeys = Enumerable.Range(0, count).Select(x => (long)x).ToList();

            while (remainingKeys.Count > 0)
            {
                var batchSize = Math.Min(rng.Next(5, 20), remainingKeys.Count);
                var delKeys = new long[batchSize];
                var delValues = new long[batchSize];

                for (int i = 0; i < batchSize; i++)
                {
                    var idx = rng.Next(0, remainingKeys.Count);
                    delKeys[i] = remainingKeys[idx];
                    delValues[i] = 0;
                    remainingKeys.RemoveAt(idx);
                }

                await bulkInserter.ApplyBatch(delKeys, delValues, batchSize, new DeleteIfExistsMutator());
            }

            var all = await ReadAllFromTree();
            Assert.Empty(all);
        }

        [Fact]
        public async Task DeleteThenReinsert()
        {
            var bulkInserter = CreateBulkInserter();

            // Insert 1,2,3
            var keys = new long[] { 1, 2, 3 };
            var values = new long[] { 10, 20, 30 };
            await bulkInserter.ApplyBatch(keys, values, 3, new UpsertMutator());

            // Delete key 2
            var delKeys = new long[] { 2 };
            var delValues = new long[] { 0 };
            await bulkInserter.ApplyBatch(delKeys, delValues, 1, new DeleteIfExistsMutator());

            // Verify key 2 is gone
            var (found, _) = await _tree.GetValue(2);
            Assert.False(found);

            // Re-insert key 2 with a different value
            var reinsertKeys = new long[] { 2 };
            var reinsertValues = new long[] { 999 };
            await bulkInserter.ApplyBatch(reinsertKeys, reinsertValues, 1, new UpsertMutator());

            var expected = new SortedDictionary<long, long>
            {
                { 1, 10 },
                { 2, 999 },
                { 3, 30 },
            };

            await AssertTreeContents(expected);
        }

        #endregion

        #region Mixed Operation Tests

        [Fact]
        public async Task MixedInsertAndUpdate_SameBatch()
        {
            var bulkInserter = CreateBulkInserter();

            // Pre-populate with keys 0, 2, 4
            var setupKeys = new long[] { 0, 2, 4 };
            var setupValues = new long[] { 0, 20, 40 };
            await bulkInserter.ApplyBatch(setupKeys, setupValues, 3, new UpsertMutator());

            // Batch with mix of existing keys (will update) and new keys (will insert)
            var keys = new long[] { 1, 2, 3, 4, 5 };
            var values = new long[] { 10, 200, 30, 400, 50 };
            await bulkInserter.ApplyBatch(keys, values, 5, new UpsertMutator());

            var expected = new SortedDictionary<long, long>
            {
                { 0, 0 },     // unchanged from setup
                { 1, 10 },    // new
                { 2, 200 },   // updated
                { 3, 30 },    // new
                { 4, 400 },   // updated
                { 5, 50 },    // new
            };

            await AssertTreeContents(expected);
        }

        #endregion

        #region NoOp Tests

        [Fact]
        public async Task NoOpMutator_TreeUnchanged()
        {
            var bulkInserter = CreateBulkInserter();

            // Pre-populate
            var keys = new long[] { 1, 2, 3 };
            var values = new long[] { 10, 20, 30 };
            await bulkInserter.ApplyBatch(keys, values, 3, new UpsertMutator());

            // Apply with no-op mutator
            var keys2 = new long[] { 1, 2, 3, 4, 5 };
            var values2 = new long[] { 100, 200, 300, 400, 500 };
            await bulkInserter.ApplyBatch(keys2, values2, 5, new NoOpMutator());

            // Tree should be unchanged
            var expected = new SortedDictionary<long, long>
            {
                { 1, 10 },
                { 2, 20 },
                { 3, 30 },
            };

            await AssertTreeContents(expected);
        }

        #endregion

        #region Multi-Batch Tests

        [Fact]
        public async Task MultipleSequentialBatches_Accumulate()
        {
            var bulkInserter = CreateBulkInserter();
            var expected = new SortedDictionary<long, long>();

            // Insert 5 batches of 20 unique keys each (non-overlapping)
            for (int batch = 0; batch < 5; batch++)
            {
                var keys = new long[20];
                var values = new long[20];
                for (int i = 0; i < 20; i++)
                {
                    var key = batch * 20 + i;
                    keys[i] = key;
                    values[i] = key * 10;
                    expected[key] = key * 10;
                }
                await bulkInserter.ApplyBatch(keys, values, 20, new UpsertMutator());
            }

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task MultipleBatches_WithOverlappingKeys()
        {
            var bulkInserter = CreateBulkInserter();

            // Batch 1: keys 0-9
            var keys1 = new long[10];
            var values1 = new long[10];
            for (int i = 0; i < 10; i++)
            {
                keys1[i] = i;
                values1[i] = i;
            }
            await bulkInserter.ApplyBatch(keys1, values1, 10, new UpsertMutator());

            // Batch 2: keys 5-14 (overlaps with 5-9)
            var keys2 = new long[10];
            var values2 = new long[10];
            for (int i = 0; i < 10; i++)
            {
                keys2[i] = i + 5;
                values2[i] = (i + 5) * 100;
            }
            await bulkInserter.ApplyBatch(keys2, values2, 10, new UpsertMutator());

            var expected = new SortedDictionary<long, long>();
            for (int i = 0; i < 5; i++)
                expected[i] = i; // From batch 1
            for (int i = 5; i < 15; i++)
                expected[i] = i * 100; // From batch 2 (overwrites 5-9 from batch 1)

            await AssertTreeContents(expected);
        }

        #endregion

        #region Bulkloader Reuse Tests

        [Fact]
        public async Task BulkInserterReuse_ArrayReuseBetweenBatches()
        {
            var bulkInserter = CreateBulkInserter();

            // Use the same arrays for multiple batches with different data
            var keys = new long[50];
            var values = new long[50];
            var expected = new SortedDictionary<long, long>();

            // Batch 1: keys 0-49
            for (int i = 0; i < 50; i++)
            {
                keys[i] = i;
                values[i] = i;
                expected[i] = i;
            }
            await bulkInserter.ApplyBatch(keys, values, 50, new UpsertMutator());

            // Batch 2: reuse arrays with different data, keys 100-119 (only first 20)
            for (int i = 0; i < 20; i++)
            {
                keys[i] = 100 + i;
                values[i] = 1000 + i;
                expected[100 + i] = 1000 + i;
            }
            await bulkInserter.ApplyBatch(keys, values, 20, new UpsertMutator());

            await AssertTreeContents(expected);
        }

        #endregion

        #region Large / Split-Triggering Tests

        [Fact]
        public async Task LargeBatch_TriggersSplits()
        {
            var bulkInserter = CreateBulkInserter();
            var count = 500;
            var keys = new long[count];
            var values = new long[count];
            var expected = new SortedDictionary<long, long>();

            for (int i = 0; i < count; i++)
            {
                keys[i] = i;
                values[i] = i * 10;
                expected[i] = i * 10;
            }

            await bulkInserter.ApplyBatch(keys, values, count, new UpsertMutator());

            await AssertTreeContents(expected);
            await AssertGetValueForAll(expected);
        }

        [Fact]
        public async Task LargeBatch_RandomUniqueKeys()
        {
            var bulkInserter = CreateBulkInserter();
            var rng = new Random(12345);
            var count = 500;
            var keys = GenerateUniqueRandomKeys(rng, count, 50000);
            var values = new long[count];
            var expected = new SortedDictionary<long, long>();

            for (int i = 0; i < count; i++)
            {
                values[i] = keys[i];
                expected[keys[i]] = keys[i];
            }

            await bulkInserter.ApplyBatch(keys, values, count, new UpsertMutator());

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task MultipleLargeBatches_UniqueKeysPerBatch()
        {
            var bulkInserter = CreateBulkInserter();
            var rng = new Random(123);
            var expected = new SortedDictionary<long, long>();
            var batchSize = 100;

            for (int batch = 0; batch < 10; batch++)
            {
                var keys = GenerateUniqueRandomKeys(rng, batchSize, 5000);
                var values = new long[batchSize];
                for (int i = 0; i < batchSize; i++)
                {
                    values[i] = keys[i] * 10 + batch;
                    expected[keys[i]] = keys[i] * 10 + batch;
                }
                await bulkInserter.ApplyBatch(keys, values, batchSize, new UpsertMutator());
            }

            await AssertTreeContents(expected);
        }

        #endregion

        #region Edge Case Tests

        [Fact]
        public async Task InsertAfterClear()
        {
            var bulkInserter = CreateBulkInserter();

            // Insert some data
            var keys = new long[] { 1, 2, 3 };
            var values = new long[] { 10, 20, 30 };
            await bulkInserter.ApplyBatch(keys, values, 3, new UpsertMutator());

            // Clear the tree
            await _tree.Clear();

            // Insert new data (need a new inserter since tree was cleared)
            var bulkInserter2 = CreateBulkInserter();
            var keys2 = new long[] { 10, 20, 30 };
            var values2 = new long[] { 100, 200, 300 };
            await bulkInserter2.ApplyBatch(keys2, values2, 3, new UpsertMutator());

            var expected = new SortedDictionary<long, long>
            {
                { 10, 100 },
                { 20, 200 },
                { 30, 300 },
            };

            await AssertTreeContents(expected);

            // Old keys should not be present
            var (found1, _) = await _tree.GetValue(1);
            Assert.False(found1);
        }

        [Fact]
        public async Task NegativeKeys()
        {
            var bulkInserter = CreateBulkInserter();
            var keys = new long[] { -10, -5, 0, 5, 10 };
            var values = new long[] { -100, -50, 0, 50, 100 };

            await bulkInserter.ApplyBatch(keys, values, 5, new UpsertMutator());

            var expected = new SortedDictionary<long, long>
            {
                { -10, -100 },
                { -5, -50 },
                { 0, 0 },
                { 5, 50 },
                { 10, 100 },
            };

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task LargeKeyValues()
        {
            var bulkInserter = CreateBulkInserter();
            var keys = new long[] { long.MinValue, -1, 0, 1, long.MaxValue };
            var values = new long[] { 1, 2, 3, 4, 5 };

            await bulkInserter.ApplyBatch(keys, values, 5, new UpsertMutator());

            var expected = new SortedDictionary<long, long>
            {
                { long.MinValue, 1 },
                { -1, 2 },
                { 0, 3 },
                { 1, 4 },
                { long.MaxValue, 5 },
            };

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task InsertSameKeyTwice_AcrossBatches()
        {
            var bulkInserter = CreateBulkInserter();

            var keys1 = new long[] { 42 };
            var values1 = new long[] { 100 };
            await bulkInserter.ApplyBatch(keys1, values1, 1, new UpsertMutator());

            var keys2 = new long[] { 42 };
            var values2 = new long[] { 200 };
            await bulkInserter.ApplyBatch(keys2, values2, 1, new UpsertMutator());

            var (found, value) = await _tree.GetValue(42);
            Assert.True(found);
            Assert.Equal(200, value);
        }

        #endregion

        #region Stress / Randomized Tests

        [Fact]
        public async Task StressTest_ManySmallBatches_UniqueKeys()
        {
            var bulkInserter = CreateBulkInserter();
            var rng = new Random(98765);
            var expected = new SortedDictionary<long, long>();
            var batchSize = 30;

            for (int batch = 0; batch < 20; batch++)
            {
                var keys = GenerateUniqueRandomKeys(rng, batchSize, 500);
                var values = new long[batchSize];

                for (int i = 0; i < batchSize; i++)
                {
                    values[i] = rng.Next(0, 10000);
                }

                // Upsert only
                await bulkInserter.ApplyBatch(keys, values, batchSize, new UpsertMutator());
                for (int i = 0; i < batchSize; i++)
                    expected[keys[i]] = values[i];
            }

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task StressTest_CompareWithIndividualRMW()
        {
            // Verifies that bulk insert produces the same result as individual Upsert.
            var rng = new Random(54321);
            var batchCount = 15;
            var batchSize = 30;
            var expected = new SortedDictionary<long, long>();

            var bulkInserter = CreateBulkInserter();

            for (int batch = 0; batch < batchCount; batch++)
            {
                // Use unique keys per batch to avoid duplicate-key-in-batch issues
                var keys = GenerateUniqueRandomKeys(rng, batchSize, 500);
                var values = new long[batchSize];

                for (int i = 0; i < batchSize; i++)
                {
                    values[i] = rng.Next(0, 1000);
                }

                await bulkInserter.ApplyBatch(keys, values, batchSize, new UpsertMutator());

                for (int i = 0; i < batchSize; i++)
                    expected[keys[i]] = values[i];
            }

            await AssertTreeContents(expected);
            await AssertGetValueForAll(expected);
        }

        #endregion

        #region Interleaving with Standard Tree Operations

        [Fact]
        public async Task BulkInsertThenIndividualUpsert()
        {
            var bulkInserter = CreateBulkInserter();

            // Bulk insert
            var keys = new long[] { 1, 3, 5 };
            var values = new long[] { 10, 30, 50 };
            await bulkInserter.ApplyBatch(keys, values, 3, new UpsertMutator());

            // Individual upserts
            await _tree.Upsert(2, 20);
            await _tree.Upsert(4, 40);

            var expected = new SortedDictionary<long, long>
            {
                { 1, 10 },
                { 2, 20 },
                { 3, 30 },
                { 4, 40 },
                { 5, 50 },
            };

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task IndividualUpsertThenBulkInsert()
        {
            // Individual upserts first
            await _tree.Upsert(2, 20);
            await _tree.Upsert(4, 40);

            // Then bulk insert
            var bulkInserter = CreateBulkInserter();
            var keys = new long[] { 1, 3, 5 };
            var values = new long[] { 10, 30, 50 };
            await bulkInserter.ApplyBatch(keys, values, 3, new UpsertMutator());

            var expected = new SortedDictionary<long, long>
            {
                { 1, 10 },
                { 2, 20 },
                { 3, 30 },
                { 4, 40 },
                { 5, 50 },
            };

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task BulkInsertThenIndividualDelete()
        {
            var bulkInserter = CreateBulkInserter();
            var keys = new long[] { 1, 2, 3, 4, 5 };
            var values = new long[] { 10, 20, 30, 40, 50 };
            await bulkInserter.ApplyBatch(keys, values, 5, new UpsertMutator());

            await _tree.Delete(3);

            var expected = new SortedDictionary<long, long>
            {
                { 1, 10 },
                { 2, 20 },
                { 4, 40 },
                { 5, 50 },
            };

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task IndividualDeleteThenBulkInsert()
        {
            // Pre-populate
            for (int i = 0; i < 10; i++)
            {
                await _tree.Upsert(i, i * 10);
            }

            // Delete some
            await _tree.Delete(3);
            await _tree.Delete(7);

            // Bulk insert with overlapping and new keys
            var bulkInserter = CreateBulkInserter();
            var keys = new long[] { 3, 7, 10, 11 };
            var values = new long[] { 300, 700, 100, 110 };
            await bulkInserter.ApplyBatch(keys, values, 4, new UpsertMutator());

            var expected = new SortedDictionary<long, long>();
            for (int i = 0; i < 10; i++)
                expected[i] = i * 10;
            expected[3] = 300;  // Re-inserted
            expected[7] = 700;  // Re-inserted
            expected[10] = 100; // New
            expected[11] = 110; // New

            await AssertTreeContents(expected);
        }

        #endregion

        #region Sorted Order Verification

        [Fact]
        public async Task TreeMaintainsSortedOrder_AfterMultipleBulkOperations()
        {
            var bulkInserter = CreateBulkInserter();
            var rng = new Random(77777);

            for (int batch = 0; batch < 10; batch++)
            {
                var size = rng.Next(10, 50);
                // unique keys per batch to avoid within-batch duplicates
                var keys = GenerateUniqueRandomKeys(rng, size, 2000);
                var values = new long[size];
                for (int i = 0; i < size; i++)
                {
                    values[i] = rng.Next();
                }
                await bulkInserter.ApplyBatch(keys, values, size, new UpsertMutator());
            }

            // Verify sorted order
            var all = await ReadAllFromTree();
            for (int i = 1; i < all.Count; i++)
            {
                Assert.True(all[i].key > all[i - 1].key,
                    $"Keys not in sorted order: key[{i - 1}]={all[i - 1].key}, key[{i}]={all[i].key}");
            }
        }

        #endregion

        #region Bug Documentation: ApplySplitsAndMerges uses wrong list for merge loop

        [Fact]
        public async Task ApplySplitsAndMerges_MergeLoopBug_Documentation()
        {
            // BPlusTreeBulkInserter.ApplySplitsAndMerges() line 136 reads:
            //   var map = _requireSplitMappings[i];
            // but it is inside the merge loop (iterating _requireMergeMappings).
            // This is a copy-paste bug — it should read:
            //   var map = _requireMergeMappings[i];
            //
            // This test documents the bug. It would fail if _requireMergeMappings.Count
            // ever exceeds _requireSplitMappings.Count during ApplySplitsAndMerges.
            //
            // For now, the split/merge path is mostly exercised via RMWNoResult
            // (GenericWriteOperation.None) which re-walks the tree to trigger
            // the built-in split/merge logic, so the bug is latent but real.

            // This test simply proves that the large-insert + split path works.
            // (Verifying the merge path would require DeleteBatch to be implemented.)
            var bulkInserter = CreateBulkInserter();
            var count = 500;
            var keys = new long[count];
            var values = new long[count];
            var expected = new SortedDictionary<long, long>();

            for (int i = 0; i < count; i++)
            {
                keys[i] = i;
                values[i] = i;
                expected[i] = i;
            }

            await bulkInserter.ApplyBatch(keys, values, count, new UpsertMutator());
            await AssertTreeContents(expected);
        }

        #endregion

        #region Edge Case: Delete-then-reinsert same key in one batch

        [Fact]
        public async Task DeleteThenReinsertSameKey_InOneBatch()
        {
            // EDGE CASE: When a key exists in the leaf and the batch contains the
            // same key twice, with a mutator that deletes on first encounter and
            // re-inserts on second, the deferred delete/insert model breaks:
            //
            // 1. First occurrence: FindIndex finds key → mutator returns Delete
            //    → queued for deletion, prevWasPendingInsert = false
            // 2. Second occurrence: prevWasPendingInsert is false → FindIndex
            //    still finds the key (delete is deferred) → mutator sees exists=true
            //    but recognizes it was already "deleted" → returns Upsert
            //    → UpdateValueAt updates the leaf value in-place (no insert queued)
            // 3. DeleteBatch runs: removes the key
            // 4. InsertFrom runs: nothing to insert for this key
            //
            // Result: key is gone when it should have been re-inserted with value 999.

            var bulkInserter = CreateBulkInserter();

            // Pre-populate key 5 with value 50
            var setupKeys = new long[] { 1, 5, 10 };
            var setupValues = new long[] { 10, 50, 100 };
            await bulkInserter.ApplyBatch(setupKeys, setupValues, 3, new UpsertMutator());

            // Verify setup
            var (foundSetup, valSetup) = await _tree.GetValue(5);
            Assert.True(foundSetup);
            Assert.Equal(50, valSetup);

            // Batch with key 5 appearing twice:
            // - First occurrence: mutator sees exists=true, deletes
            // - Second occurrence: mutator should re-insert with value 999
            var keys = new long[] { 5, 5 };
            var values = new long[] { 0, 999 };
            var mutator = new DeleteThenReinsertMutator();
            await bulkInserter.ApplyBatch(keys, values, 2, mutator);

            // Expected: key 5 should exist with value 999
            // Keys 1 and 10 should be untouched
            var expected = new SortedDictionary<long, long>
            {
                { 1, 10 },
                { 5, 999 },
                { 10, 100 },
            };

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task DeleteThenReinsertSameKey_InOneBatch_MultipleKeys()
        {
            // Same edge case but with multiple keys being delete-then-reinserted.

            var bulkInserter = CreateBulkInserter();

            // Pre-populate keys 1-5
            var setupKeys = new long[] { 1, 2, 3, 4, 5 };
            var setupValues = new long[] { 10, 20, 30, 40, 50 };
            await bulkInserter.ApplyBatch(setupKeys, setupValues, 5, new UpsertMutator());

            // Delete-then-reinsert keys 2 and 4
            var keys = new long[] { 2, 2, 4, 4 };
            var values = new long[] { 0, 222, 0, 444 };
            var mutator = new DeleteThenReinsertMutator();
            await bulkInserter.ApplyBatch(keys, values, 4, mutator);

            var expected = new SortedDictionary<long, long>
            {
                { 1, 10 },
                { 2, 222 },
                { 3, 30 },
                { 4, 444 },
                { 5, 50 },
            };

            await AssertTreeContents(expected);
        }

        #endregion

        #region Edge Case: Insert-then-delete of non-existent key in one batch

        [Fact]
        public async Task InsertThenDeleteNonExistentKey_InOneBatch()
        {
            // Key does NOT pre-exist. Batch inserts then deletes it.
            // ToggleMutator: !exists → Upsert, exists → Delete.
            //
            // i=0: key=5, not found → Upsert → pending insert
            // i=1: key=5, dup, prevWasPendingInsert → Delete → cancel pending insert
            //
            // Expected: key 5 should NOT exist in tree.

            var bulkInserter = CreateBulkInserter();

            var keys = new long[] { 5, 5 };
            var values = new long[] { 100, 200 };

            await bulkInserter.ApplyBatch(keys, values, 2, new ToggleMutator());

            var (found, _) = await _tree.GetValue(5);
            Assert.False(found, "Key 5 should not exist after insert→delete in same batch");

            var all = await ReadAllFromTree();
            Assert.Empty(all);
        }

        [Fact]
        public async Task InsertThenDeleteNonExistentKey_WithOtherKeysAfter()
        {
            // Same as above but with other keys following the canceled insert-delete.
            // This exposes the insertOffset bug: canceling a pending insert should NOT
            // decrement insertOffset since no element was removed from the leaf.
            //
            // keys=[5, 5, 10], sorted: [5, 5, 10]
            // i=0: key=5, not found → Upsert → pending insert
            // i=1: key=5, dup → Delete → cancel pending insert
            // i=2: key=10, not found → Upsert → insert queued
            //      target position = leafIndex + insertOffset
            //      BUG: if insertOffset was incorrectly -1, position is shifted wrong

            var bulkInserter = CreateBulkInserter();

            var keys = new long[] { 5, 5, 10 };
            var values = new long[] { 100, 200, 1000 };

            await bulkInserter.ApplyBatch(keys, values, 3, new ToggleMutator());

            // Key 5 should NOT exist (inserted then deleted)
            var (found5, _) = await _tree.GetValue(5);
            Assert.False(found5, "Key 5 should not exist");

            // Key 10 should exist with value 1000
            var (found10, val10) = await _tree.GetValue(10);
            Assert.True(found10, "Key 10 should exist");
            Assert.Equal(1000, val10);

            var all = await ReadAllFromTree();
            Assert.Single(all);
        }

        [Fact]
        public async Task InsertDeleteInsertNonExistentKey_InOneBatch()
        {
            // Key does NOT pre-exist. Batch has key 3 times: insert → delete → insert.
            // ToggleMutator: !exists → Upsert, exists → Delete.
            //
            // i=0: key=5, not found → Upsert → pending insert
            // i=1: key=5, dup, prevWasPendingInsert → Delete → cancel pending insert
            // i=2: key=5, dup, ??? → should see exists=false → Upsert
            //
            // Expected: key 5 should exist with the third value.

            var bulkInserter = CreateBulkInserter();

            var keys = new long[] { 5, 5, 5 };
            var values = new long[] { 100, 200, 300 };

            await bulkInserter.ApplyBatch(keys, values, 3, new ToggleMutator());

            // Key 5 should exist (insert → delete → insert)
            var (found, val) = await _tree.GetValue(5);
            Assert.True(found, "Key 5 should exist after insert→delete→insert");
            Assert.Equal(300, val);

            var all = await ReadAllFromTree();
            Assert.Single(all);
        }

        [Fact]
        public async Task InsertDeleteInsertNonExistentKey_WithOtherKeys()
        {
            // Mix of: key 3 (unique), key 5 (insert→delete→insert), key 10 (unique).

            var bulkInserter = CreateBulkInserter();

            var keys = new long[] { 3, 5, 5, 5, 10 };
            var values = new long[] { 30, 100, 200, 300, 1000 };

            await bulkInserter.ApplyBatch(keys, values, 5, new ToggleMutator());

            var expected = new SortedDictionary<long, long>
            {
                { 3, 30 },
                { 5, 300 },  // survived insert→delete→insert
                { 10, 1000 },
            };

            await AssertTreeContents(expected);
        }

        [Fact]
        public async Task LongToggleChain_NonExistentKey_OddCount()
        {
            // 9 occurrences of same non-existent key with ToggleMutator.
            // Odd count → final state is "inserted".
            var bulkInserter = CreateBulkInserter();

            var keys = Enumerable.Repeat(5L, 9).ToArray();
            var values = Enumerable.Range(0, 9).Select(i => (long)(i * 100)).ToArray();

            await bulkInserter.ApplyBatch(keys, values, 9, new ToggleMutator());

            var (found, val) = await _tree.GetValue(5);
            Assert.True(found, "Odd toggle count should leave key inserted");
            Assert.Equal(800, val); // Last value

            var all = await ReadAllFromTree();
            Assert.Single(all);
        }

        [Fact]
        public async Task LongToggleChain_NonExistentKey_EvenCount()
        {
            // 10 occurrences → even count → final state is "not inserted".
            var bulkInserter = CreateBulkInserter();

            var keys = Enumerable.Repeat(5L, 10).ToArray();
            var values = Enumerable.Range(0, 10).Select(i => (long)(i * 100)).ToArray();

            await bulkInserter.ApplyBatch(keys, values, 10, new ToggleMutator());

            var (found, _) = await _tree.GetValue(5);
            Assert.False(found, "Even toggle count should leave key absent");

            var all = await ReadAllFromTree();
            Assert.Empty(all);
        }

        [Fact]
        public async Task LongToggleChain_ExistingKey()
        {
            // Key pre-exists. ToggleMutator on existing key: exists=true → Delete.
            // So the first toggle deletes, the second (exists=false) re-inserts.
            // Each pair is delete→cancel = no-op.
            // 6 toggles = 3 no-op pairs → key SURVIVES.
            var bulkInserter = CreateBulkInserter();

            var setup = new long[] { 5 };
            await bulkInserter.ApplyBatch(setup, new long[] { 50 }, 1, new UpsertMutator());

            var keys = Enumerable.Repeat(5L, 6).ToArray();
            var values = Enumerable.Range(0, 6).Select(i => (long)(i * 100)).ToArray();

            await bulkInserter.ApplyBatch(keys, values, 6, new ToggleMutator());

            // Even pairs: delete→cancel, delete→cancel, delete→cancel = key survives
            var (found, val) = await _tree.GetValue(5);
            Assert.True(found, "Even toggle pairs on existing key = no-op, key survives");
            Assert.Equal(500, val); // Last cancel used value at index 5
        }

        [Fact]
        public async Task MixedToggleChain_WithSurroundingKeys()
        {
            // Batch with unique keys + toggling keys, all interleaved after sorting.
            var bulkInserter = CreateBulkInserter();

            // Pre-populate key 5
            await bulkInserter.ApplyBatch(new long[] { 5 }, new long[] { 50 }, 1, new UpsertMutator());

            // Batch: key 1 (new), key 3 (new x3 = insert→delete→insert),
            //        key 5 (exists x2 = delete→cancel = survives),
            //        key 8 (new x2 = insert→delete = gone), key 10 (new)
            var keys   = new long[] { 1,  3, 3, 3,  5, 5,  8, 8,  10 };
            var values = new long[] { 10, 30, 31, 32, 51, 52, 80, 81, 100 };

            await bulkInserter.ApplyBatch(keys, values, 9, new ToggleMutator());

            var expected = new SortedDictionary<long, long>
            {
                { 1, 10 },
                { 3, 32 },   // insert→delete→insert: survives
                { 5, 52 },   // exists: delete→cancel (Upsert with value 52)
                // 8: insert→delete: gone
                { 10, 100 },
            };

            await AssertTreeContents(expected);
        }

        #endregion

        public void Dispose()
        {
            _stateManager?.Dispose();
        }
    }
}
