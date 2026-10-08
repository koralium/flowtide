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
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using System.Diagnostics;
using KeyContainer = FlowtideDotNet.Storage.Tree.PrimitiveListKeyContainer<int>;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    /// <param name="ClearTime">Set when the slice finished the migration and cleared the old generation.</param>
    internal readonly record struct MigrationWork(int Records, long Bytes, TimeSpan SliceTime, TimeSpan? ClearTime);

    /// <summary>
    /// An uncommitted tree keyed by file id in two fixed generations.
    /// An uncommitted tree keeps bookkeeping for every page it ever touched, so after enough churn the live records
    /// move to the other generation in slices and the old one is cleared, which drops that bookkeeping.
    /// </summary>
    internal sealed class RotatingTree<V>
        where V : class
    {
        private readonly IBPlusTree<int, V, KeyContainer, ManagedValueContainer<V>>[] _generations;
        private readonly Func<V, int> _sizeOf;
        private readonly PrimitiveListComparer<int> _comparer = new PrimitiveListComparer<int>();
        private int _current;
        private bool _migrating;
        // Next key of the old generation to copy
        private int _cursor;
        private long _mutations;

        private RotatingTree(IBPlusTree<int, V, KeyContainer, ManagedValueContainer<V>>[] generations, Func<V, int> sizeOf)
        {
            _generations = generations;
            _sizeOf = sizeOf;
        }

        public static async Task<RotatingTree<V>> Open(IStateManagerClient client, string name, IBplusTreeValueSerializer<V, ManagedValueContainer<V>> serializer, IMemoryAllocator memoryAllocator)
        {
            var generations = new IBPlusTree<int, V, KeyContainer, ManagedValueContainer<V>>[2];
            for (int i = 0; i < 2; i++)
            {
                generations[i] = await client.GetOrCreateTree($"{name}_{i}", new BPlusTreeOptions<int, V, KeyContainer, ManagedValueContainer<V>>()
                {
                    Comparer = new PrimitiveListComparer<int>(),
                    KeySerializer = new PrimitiveListKeyContainerSerializer<int>(memoryAllocator),
                    ValueSerializer = serializer,
                    MemoryAllocator = memoryAllocator,
                    UseByteBasedPageSizes = true
                });
                // Never committed, a restart already emptied it
                await generations[i].Clear();
            }
            return new RotatingTree<V>(generations, serializer.CreateEmpty().SizeOf);
        }

        private IBPlusTree<int, V, KeyContainer, ManagedValueContainer<V>> Current => _generations[_current];

        private IBPlusTree<int, V, KeyContainer, ManagedValueContainer<V>> Old => _generations[1 - _current];

        public bool Migrating => _migrating;

        public long Mutations => _mutations;

        public long RecordsMigrated { get; private set; }

        public long BytesMigrated { get; private set; }

        public int Rotations { get; private set; }

        // Migrations that copied everything and cleared the old generation
        public int Completed { get; private set; }

        public async Task Upsert(int id, V value)
        {
            _mutations++;
            await Current.Upsert(id, value);
            if (_migrating)
            {
                await Old.Delete(id);
            }
        }

        public async Task Delete(int id)
        {
            _mutations++;
            await Current.Delete(id);
            if (_migrating)
            {
                await Old.Delete(id);
            }
        }

        /// <summary>
        /// Values of the ids that exist, the current generation wins over the one being migrated.
        /// </summary>
        public async Task<Dictionary<int, V>> Get(IReadOnlyCollection<int> ids)
        {
            var found = new Dictionary<int, V>(ids.Count);
            if (ids.Count == 0)
            {
                return found;
            }
            await Find(Current, ids.ToArray(), found);
            if (_migrating && found.Count < ids.Count)
            {
                await Find(Old, ids.Where(x => !found.ContainsKey(x)).ToArray(), found);
            }
            return found;
        }

        private async Task Find(IBPlusTree<int, V, KeyContainer, ManagedValueContainer<V>> tree, int[] ids, Dictionary<int, V> found)
        {
            if (ids.Length == 0)
            {
                return;
            }
            using var searcher = tree.CreateBulkSearcher(_comparer);
            await searcher.Start(ids, ids.Length);
            while (await searcher.MoveNextLeaf())
            {
                var leaf = searcher.CurrentLeaf;
                foreach (var result in searcher.CurrentResults)
                {
                    if (result.Found)
                    {
                        found[ids[result.KeyIndex]] = leaf.values.Get(result.LowerBound);
                    }
                }
            }
        }

        /// <summary>
        /// Every record in key order, the current generation wins, the tree must not change during the scan.
        /// </summary>
        public async IAsyncEnumerable<(int Id, V Value)> ScanAll()
        {
            if (!_migrating)
            {
                await foreach (var entry in Entries(Current, int.MinValue))
                {
                    yield return entry;
                }
                yield break;
            }
            await using var current = Entries(Current, int.MinValue).GetAsyncEnumerator();
            await using var old = Entries(Old, _cursor).GetAsyncEnumerator();
            var hasCurrent = await current.MoveNextAsync();
            var hasOld = await old.MoveNextAsync();
            while (hasCurrent || hasOld)
            {
                if (hasCurrent && (!hasOld || current.Current.Id <= old.Current.Id))
                {
                    if (hasOld && current.Current.Id == old.Current.Id)
                    {
                        hasOld = await old.MoveNextAsync();
                    }
                    yield return current.Current;
                    hasCurrent = await current.MoveNextAsync();
                }
                else
                {
                    yield return old.Current;
                    hasOld = await old.MoveNextAsync();
                }
            }
        }

        private static async IAsyncEnumerable<(int Id, V Value)> Entries(IBPlusTree<int, V, KeyContainer, ManagedValueContainer<V>> tree, int from)
        {
            using var iterator = tree.CreateIterator();
            await iterator.Seek(from);
            await foreach (var page in iterator)
            {
                foreach (var kv in page)
                {
                    yield return (kv.Key, kv.Value);
                }
            }
        }

        /// <summary>
        /// Starts a rotation once the mutations since the last one exceed twice the live records, and the floor.
        /// </summary>
        public void RotateIfDue(long liveRecords, long floor)
        {
            if (_migrating || _mutations <= Math.Max(2 * liveRecords, floor))
            {
                return;
            }
            _current = 1 - _current;
            _migrating = true;
            _cursor = int.MinValue;
            _mutations = 0;
            Rotations++;
        }

        /// <summary>
        /// Copies records into the current generation within both budgets, a key already there is newer.
        /// With <paramref name="first"/> a record larger than the byte budget still moves, alone. The old generation is cleared once it is copied.
        /// </summary>
        public async Task<MigrationWork> MigrateSlice(int maxRecords, long maxBytes, bool first = true)
        {
            if (!_migrating || maxRecords <= 0 || maxBytes <= 0)
            {
                return default;
            }
            var started = Stopwatch.GetTimestamp();
            // Copied out first, an iterator must not be held across mutations
            var slice = new List<(int Id, V Value)>();
            long bytes = 0;
            var more = false;
            await foreach (var entry in Entries(Old, _cursor))
            {
                var size = _sizeOf(entry.Value);
                var fits = slice.Count < maxRecords && bytes + size <= maxBytes;
                if (!fits && !(first && slice.Count == 0))
                {
                    more = true;
                    break;
                }
                slice.Add(entry);
                bytes += size;
            }
            foreach (var (id, value) in slice)
            {
                await Current.RMWNoResult(id, value, static (input, current, exists) => exists ? (current, GenericWriteOperation.None) : (input, GenericWriteOperation.Upsert));
            }
            RecordsMigrated += slice.Count;
            BytesMigrated += bytes;
            var sliceTime = Stopwatch.GetElapsedTime(started);
            if (more)
            {
                if (slice.Count > 0)
                {
                    _cursor = slice[^1].Id + 1;
                }
                return new MigrationWork(slice.Count, bytes, sliceTime, null);
            }
            var clearStarted = Stopwatch.GetTimestamp();
            await Old.Clear();
            _migrating = false;
            Completed++;
            return new MigrationWork(slice.Count, bytes, sliceTime, Stopwatch.GetElapsedTime(clearStarted));
        }

        public async Task Clear()
        {
            await _generations[0].Clear();
            await _generations[1].Clear();
            _migrating = false;
            _mutations = 0;
        }
    }
}
