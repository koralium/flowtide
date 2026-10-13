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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using Stowage;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// What a build may read from the cache: the storage, the table id and the weak floor it runs under.
    /// </summary>
    internal sealed record RootCacheScope(RootCache Cache, IFileStorage Storage, string TableId, long WeakFloor);

    /// <summary>
    /// Local copies of downloaded roots and our own outputs, keyed by storage, table, table id and version, and valid only under the identity they were read with.
    /// </summary>
    internal sealed class RootCache
    {
        public const int DefaultMaxEntries = 256;
        public const long DefaultMaxMetadataBytes = 4L << 20;

        // Per entry beyond its strings
        private const long EntryOverhead = 128;

        private readonly object _lock = new object();
        private readonly ConditionalWeakTable<IFileStorage, StorageId> _storageIds = new ConditionalWeakTable<IFileStorage, StorageId>();
        private readonly Dictionary<Key, Entry> _entries = new Dictionary<Key, Entry>();
        private readonly LinkedList<Entry> _lru = new LinkedList<Entry>();
        private readonly ScratchDirectory _directory;
        private readonly Func<long> _nextSequence;
        private long _storageCounter;
        private long _metadataBytes;

        public RootCache(ScratchDirectory directory, Func<long> nextSequence, int maxEntries = DefaultMaxEntries, long maxMetadataBytes = DefaultMaxMetadataBytes)
        {
            _directory = directory;
            _nextSequence = nextSequence;
            MaxEntries = maxEntries;
            MaxMetadataBytes = maxMetadataBytes;
            DirectoryPath = Path.Combine(directory.ProcessPath, "cache");
        }

        public string DirectoryPath { get; }

        public int MaxEntries { get; }

        public long MaxMetadataBytes { get; }

        public int Count
        {
            get
            {
                lock (_lock)
                {
                    return _entries.Count;
                }
            }
        }

        public long MetadataBytes
        {
            get
            {
                lock (_lock)
                {
                    return _metadataBytes;
                }
            }
        }

        internal sealed class StorageId
        {
            public required long Value { get; init; }

            public required WeakReference<IFileStorage> Storage { get; init; }
        }

        internal readonly record struct Key(long Storage, string Table, string TableId, long Version);

        internal sealed class Entry
        {
            public required Key Key { get; init; }

            public required StorageId Storage { get; init; }

            public required string Path { get; init; }

            public required long Charge { get; init; }

            public required ObjectIdentity Identity { get; init; }

            public required long Sequence { get; init; }

            public required long MetadataBytes { get; init; }

            public int Pins;

            // Removed from the cache while pinned; deleted by the last unpin
            public bool Doomed;

            // Claimed under the lock by whoever deletes the file, so its charge is released once
            public bool Deleted;

            public LinkedListNode<Entry>? Node;
        }

        /// <summary>
        /// A pinned cache file; eviction never deletes it while the lease is open.
        /// </summary>
        internal sealed class Lease : IDisposable
        {
            private RootCache? _cache;
            private readonly Entry _entry;

            internal Lease(RootCache cache, Entry entry)
            {
                _cache = cache;
                _entry = entry;
            }

            public string Path => _entry.Path;

            public ObjectIdentity Identity => _entry.Identity;

            public void Dispose()
            {
                Interlocked.Exchange(ref _cache, null)?.Unpin(_entry);
            }
        }

        private StorageId IdOf(IFileStorage storage)
        {
            return _storageIds.GetValue(storage, x => new StorageId() { Value = Interlocked.Increment(ref _storageCounter), Storage = new WeakReference<IFileStorage>(x) });
        }

        private Key KeyOf(RootCacheScope scope, ObjectIdentity identity) => new Key(IdOf(scope.Storage).Value, identity.Table, scope.TableId, identity.Version);

        /// <summary>
        /// A pinned hit only for the same identity, and for a weak identity only above the weak floor; any other entry for the key is dropped.
        /// </summary>
        public Lease? TryGet(RootCacheScope scope, ObjectIdentity identity)
        {
            Entry? stale = null;
            Lease? lease = null;
            lock (_lock)
            {
                if (!_entries.TryGetValue(KeyOf(scope, identity), out var entry))
                {
                    return null;
                }
                if (entry.Identity != identity || (!entry.Identity.Strong && entry.Sequence <= scope.WeakFloor) || !File.Exists(entry.Path))
                {
                    stale = RemoveLocked(entry);
                }
                else
                {
                    entry.Pins++;
                    _lru.Remove(entry.Node!);
                    _lru.AddLast(entry.Node!);
                    lease = new Lease(this, entry);
                }
            }
            if (stale != null)
            {
                Delete(stale);
            }
            return lease;
        }

        /// <summary>
        /// Moves a job's file into the cache under its identity and returns it pinned; null when the move fails, and the job keeps the file.
        /// </summary>
        public Lease? Put(RootCacheScope scope, ObjectIdentity identity, JobScratch scratch, string path, bool supersede = false)
        {
            var target = System.IO.Path.Combine(DirectoryPath, Guid.NewGuid().ToString("N") + ".parquet");
            var charge = scratch.Release(path);
            try
            {
                Directory.CreateDirectory(DirectoryPath);
                File.Move(path, target);
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException)
            {
                scratch.Adopt(path, charge);
                return null;
            }
            var storage = IdOf(scope.Storage);
            var entry = new Entry()
            {
                Key = new Key(storage.Value, identity.Table, scope.TableId, identity.Version),
                Storage = storage,
                Path = target,
                Charge = charge,
                Identity = identity,
                Sequence = _nextSequence(),
                MetadataBytes = EntryOverhead + 2L * (identity.Table.Length + scope.TableId.Length + target.Length + (identity.Md5?.Length ?? 0)),
                Pins = 1
            };
            var dropped = new List<Entry>();
            lock (_lock)
            {
                if (_entries.TryGetValue(entry.Key, out var previous))
                {
                    dropped.Add(RemoveLocked(previous));
                }
                // A published and validated root is where later builds start, so the table's older roots only hold disk
                if (supersede)
                {
                    foreach (var older in _lru.Where(x => x.Key.Storage == entry.Key.Storage && x.Key.Table == entry.Key.Table && x.Key.TableId == entry.Key.TableId && x.Key.Version < entry.Key.Version).ToList())
                    {
                        dropped.Add(RemoveLocked(older));
                    }
                }
                _entries[entry.Key] = entry;
                entry.Node = _lru.AddLast(entry);
                _metadataBytes += entry.MetadataBytes;
                dropped.AddRange(TrimLocked(0));
            }
            foreach (var x in dropped)
            {
                Delete(x);
            }
            return new Lease(this, entry);
        }

        /// <summary>
        /// Drops the entry for a root whose bytes failed to decode.
        /// </summary>
        public void Drop(RootCacheScope scope, ObjectIdentity identity)
        {
            Entry? removed = null;
            lock (_lock)
            {
                if (_entries.TryGetValue(KeyOf(scope, identity), out var entry))
                {
                    removed = RemoveLocked(entry);
                }
            }
            if (removed != null)
            {
                Delete(removed);
            }
        }

        /// <summary>
        /// Evicts unpinned entries, dead storages first, until the given bytes of disk are freed or nothing is left to evict; returns the bytes freed.
        /// </summary>
        public long EvictFor(long bytes)
        {
            long freed = 0;
            // A failed deletion keeps its charge, so the next entries are tried
            while (freed < bytes)
            {
                List<Entry> evicted;
                lock (_lock)
                {
                    evicted = TrimLocked(bytes - freed);
                }
                if (evicted.Count == 0)
                {
                    break;
                }
                foreach (var entry in evicted)
                {
                    if (Delete(entry))
                    {
                        freed += entry.Charge;
                    }
                }
            }
            return freed;
        }

        // Dead storages always go; then least recently used entries until the caps hold and the given bytes are covered
        private List<Entry> TrimLocked(long bytes)
        {
            var evicted = new List<Entry>();
            foreach (var entry in _lru.Where(x => x.Pins == 0 && !x.Storage.Storage.TryGetTarget(out _)).ToList())
            {
                evicted.Add(RemoveLocked(entry));
            }
            long covered = evicted.Sum(x => x.Charge);
            var node = _lru.First;
            while (node != null && (_entries.Count > MaxEntries || _metadataBytes > MaxMetadataBytes || covered < bytes))
            {
                var next = node.Next;
                if (node.Value.Pins == 0)
                {
                    covered += node.Value.Charge;
                    evicted.Add(RemoveLocked(node.Value));
                }
                node = next;
            }
            return evicted;
        }

        private Entry RemoveLocked(Entry entry)
        {
            _entries.Remove(entry.Key);
            _lru.Remove(entry.Node!);
            _metadataBytes -= entry.MetadataBytes;
            entry.Doomed = true;
            return entry;
        }

        private void Unpin(Entry entry)
        {
            bool delete;
            List<Entry>? trimmed = null;
            lock (_lock)
            {
                entry.Pins--;
                delete = entry.Pins == 0 && entry.Doomed && !entry.Deleted;
                entry.Deleted |= delete;
                // Pinned entries outlast the caps until their last unpin
                if (entry.Pins == 0 && !entry.Doomed && (_entries.Count > MaxEntries || _metadataBytes > MaxMetadataBytes))
                {
                    trimmed = TrimLocked(0);
                }
            }
            if (delete)
            {
                DeleteNow(entry);
            }
            if (trimmed != null)
            {
                foreach (var x in trimmed)
                {
                    Delete(x);
                }
            }
        }

        // A pinned entry is deleted by its last unpin
        private bool Delete(Entry entry)
        {
            lock (_lock)
            {
                if (entry.Pins > 0 || entry.Deleted)
                {
                    return false;
                }
                entry.Deleted = true;
            }
            return DeleteNow(entry);
        }

        // The charge goes only after the file is gone; a failed deletion keeps it on the pending list
        private bool DeleteNow(Entry entry)
        {
            if (_directory.TryDeleteFile(entry.Path))
            {
                _directory.Ledger.Release(entry.Charge);
                return true;
            }
            _directory.QueuePending(entry.Path, entry.Charge);
            return false;
        }
    }
}
