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

using FlowtideDotNet.Storage.FileCache;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;

namespace FlowtideDotNet.Storage.Tests
{
    /// <summary>
    /// Wraps the built-in file cache, records written and freed keys, and can hold a write before the inner write.
    /// </summary>
    internal sealed class SpyFileCache : IFileCache
    {
        private readonly object _lock = new object();
        private readonly List<long> _written = new List<long>();
        private readonly List<long> _freed = new List<long>();
        private long _gatedKey;
        private ManualResetEventSlim? _gate;
        private TaskCompletionSource? _gateEntered;

        public SpyFileCache(FlowtideDotNet.Storage.FileCache.FileCache inner)
        {
            Inner = inner;
        }

        public FlowtideDotNet.Storage.FileCache.FileCache Inner { get; }

        public List<long> WrittenKeys
        {
            get
            {
                lock (_lock)
                {
                    return new List<long>(_written);
                }
            }
        }

        public List<long> FreedKeys
        {
            get
            {
                lock (_lock)
                {
                    return new List<long>(_freed);
                }
            }
        }

        // Holds the next write of the key before the inner write, the writer keeps whatever it holds
        public Task ArmWriteGate(long key, ManualResetEventSlim gate)
        {
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (_lock)
            {
                _gatedKey = key;
                _gateEntered = entered;
                _gate = gate;
            }
            return entered.Task;
        }

        public void Write(long id, SerializableObject serializableObject)
        {
            ManualResetEventSlim? gate = null;
            TaskCompletionSource? entered = null;
            lock (_lock)
            {
                _written.Add(id);
                if (_gate != null && _gatedKey == id)
                {
                    gate = _gate;
                    entered = _gateEntered;
                    _gate = null;
                    _gateEntered = null;
                }
            }
            if (gate != null)
            {
                entered!.TrySetResult();
                if (!gate.Wait(TimeSpan.FromMinutes(2)))
                {
                    throw new TimeoutException($"Write gate for page {id} was never opened");
                }
            }
            Inner.Write(id, serializableObject);
        }

        public ValueTask<ReadOnlyMemory<byte>> Read(long pageKey) => Inner.Read(pageKey);

        public ValueTask<T> Read<T>(long pageKey, IStateSerializer<T> serializer)
            where T : ICacheObject => Inner.Read(pageKey, serializer);

        // Frees that released a spill allocation
        public long FreedAllocations => Interlocked.Read(ref _freedAllocations);

        private long _freedAllocations;

        public void Free(in long pageKey)
        {
            lock (_lock)
            {
                _freed.Add(pageKey);
            }
            if (Inner.Exists(pageKey))
            {
                Interlocked.Increment(ref _freedAllocations);
            }
            Inner.Free(pageKey);
        }

        public void FreeAll(IEnumerable<long> keys)
        {
            var list = keys.ToList();
            lock (_lock)
            {
                _freed.AddRange(list);
            }
            foreach (var key in list)
            {
                if (Inner.Exists(key))
                {
                    Interlocked.Increment(ref _freedAllocations);
                }
            }
            Inner.FreeAll(list);
        }

        public void Flush() => Inner.Flush();

        public void ClearTemporaryAllocations() => Inner.ClearTemporaryAllocations();

        public void Dispose() => Inner.Dispose();
    }

    internal sealed class SpyFileCacheFactory : IFileCacheFactory
    {
        private readonly FileCacheOptions _options;

        public SpyFileCacheFactory(FileCacheOptions options)
        {
            _options = options;
        }

        public FileCacheOptions Options => _options;

        public IFileCache Create(string name, IMemoryAllocator memoryAllocator)
        {
            return new SpyFileCache(new FlowtideDotNet.Storage.FileCache.FileCache(_options, name, memoryAllocator));
        }
    }

    /// <summary>
    /// A manager over in-memory Reservoir storage with its cleanup task stopped and spy file caches.
    /// </summary>
    internal sealed class EphemeralHarness : IDisposable
    {
        private readonly string _name;
        private readonly int _cachePageCount;
        private readonly bool _useReadCache;
        private readonly bool _zstd;
        private readonly bool _directIO;
        private readonly bool _backgroundCommit;
        private readonly int _pageSize;
        private readonly int _pageSizeBytes;
        private int _generation;
        // Retired storages stay open, a new one missed the checkpoint once the old one was disposed first
        private readonly List<BackgroundCommitTests.RecordingStorage> _retired = new List<BackgroundCommitTests.RecordingStorage>();

        private EphemeralHarness(string name, int cachePageCount, bool useReadCache, bool zstd, bool directIO, bool backgroundCommit, int pageSize, int pageSizeBytes, MemoryFileProvider fileProvider)
        {
            _name = name;
            _cachePageCount = cachePageCount;
            _useReadCache = useReadCache;
            _zstd = zstd;
            _directIO = directIO;
            _backgroundCommit = backgroundCommit;
            _pageSize = pageSize;
            _pageSizeBytes = pageSizeBytes;
            FileProvider = fileProvider;
        }

        public MemoryFileProvider FileProvider { get; }

        public StateManagerSync<StateManagerMetadata> Manager { get; private set; } = null!;

        public BackgroundCommitTests.RecordingStorage Storage { get; private set; } = null!;

        public SpyFileCacheFactory Files { get; private set; } = null!;

        public string TempDirectory => $"./data/ephemeral_{_name}/temp{_generation}";

        public static async Task<EphemeralHarness> Create(string name, int cachePageCount = 1000, bool useReadCache = false, bool zstd = false, bool directIO = false, bool backgroundCommit = true, int pageSize = 128, int pageSizeBytes = 32 * 1024, MemoryFileProvider? fileProvider = null)
        {
            var harness = new EphemeralHarness(name, cachePageCount, useReadCache, zstd, directIO, backgroundCommit, pageSize, pageSizeBytes, fileProvider ?? new MemoryFileProvider());
            await harness.Start();
            return harness;
        }

        private async Task Start()
        {
            Storage = new BackgroundCommitTests.RecordingStorage(new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = FileProvider }));
            var tempOptions = new FileCacheOptions() { DirectoryPath = TempDirectory, UseDirectIOOnLinux = _directIO };
            Files = new SpyFileCacheFactory(tempOptions);
            var serializeOptions = new StateSerializeOptions();
            if (_zstd)
            {
                serializeOptions.CompressionType = CompressionType.Zstd;
                serializeOptions.CompressionMethod = CompressionMethod.Page;
            }
            var options = new StateManagerOptions()
            {
                PersistentStorage = Storage,
                CachePageCount = _cachePageCount,
                MinCachePageCount = _cachePageCount,
                UseReadCache = _useReadCache,
                BackgroundCommit = _backgroundCommit,
                TemporaryStorageOptions = tempOptions,
                FileCacheFactory = Files,
                SerializeOptions = serializeOptions,
                DefaultBPlusTreePageSize = _pageSize,
                DefaultBPlusTreePageSizeBytes = _pageSizeBytes,
            };
            Manager = new StateManagerSync<StateManagerMetadata>(options, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter($"ephemeral_{_name}_{_generation}"), _name, GlobalMemoryManager.Instance);
            await Manager.InitializeAsync();
            await Manager.CacheTable.StopCleanupTask();
        }

        // Disposes the manager and starts a new one over the same persisted files
        public async Task Restart()
        {
            Manager.Dispose();
            _retired.Add(Storage);
            _generation++;
            await Start();
        }

        public void Dispose()
        {
            Manager.Dispose();
            Storage.Dispose();
            foreach (var storage in _retired)
            {
                storage.Dispose();
            }
        }
    }

    internal static class TreeWalk
    {
        internal sealed record Reachable(HashSet<long> Pages, int InternalNodes);

        // Walks from the root and returns each fetch rent, so only the cache keeps a rent
        public static async Task<Reachable> ReachablePages<K, V, TKeyContainer, TValueContainer>(BPlusTree<K, V, TKeyContainer, TValueContainer> tree)
            where TKeyContainer : IKeyContainer<K>
            where TValueContainer : IValueContainer<V>
        {
            var client = tree.m_stateClient;
            var pages = new HashSet<long>();
            var internalNodes = 0;
            var stack = new Stack<long>();
            stack.Push(client.Metadata!.Root);
            while (stack.Count > 0)
            {
                var id = stack.Pop();
                Assert.True(pages.Add(id), $"page {id} is reachable twice");
                var node = await client.GetValue(id);
                Assert.NotNull(node);
                try
                {
                    if (node is InternalNode<K, V, TKeyContainer> internalNode)
                    {
                        internalNodes++;
                        for (int i = 0; i < internalNode.children.Count; i++)
                        {
                            stack.Push(internalNode.children[i]);
                        }
                    }
                }
                finally
                {
                    node!.Return();
                }
            }
            return new Reachable(pages, internalNodes);
        }
    }

    /// <summary>
    /// ASCII string values that report their serialized size, so byte based pages split on value size.
    /// </summary>
    internal sealed class SizedStringValueContainer : IValueContainer<string>
    {
        internal readonly List<string> _values = new List<string>();

        public int Count => _values.Count;

        public static int EntrySize(string value) => sizeof(int) + value.Length;

        public void AddRangeFrom(IValueContainer<string> container, int start, int count)
        {
            _values.AddRange(((SizedStringValueContainer)container)._values.GetRange(start, count));
        }

        public void DeleteBatch(ReadOnlySpan<int> positions)
        {
            for (int i = positions.Length - 1; i >= 0; i--)
            {
                _values.RemoveAt(positions[i]);
            }
        }

        public void Dispose()
        {
        }

        public string Get(int index) => _values[index];

        public int GetByteSize() => GetByteSize(0, _values.Count);

        public int GetByteSize(int start, int end)
        {
            var size = 0;
            for (int i = start; i < end; i++)
            {
                size += EntrySize(_values[i]);
            }
            return size;
        }

        public ref string GetRef(int index) => ref CollectionsMarshal.AsSpan(_values)[index];

        public void Insert(int index, string value) => _values.Insert(index, value);

        public void InsertFrom(string[] values, ReadOnlySpan<int> sortedLookup, ReadOnlySpan<int> targetPositions)
        {
            for (int i = sortedLookup.Length - 1; i >= 0; i--)
            {
                _values.Insert(targetPositions[i], values[sortedLookup[i]]);
            }
        }

        public void RemoveAt(int index) => _values.RemoveAt(index);

        public void RemoveRange(int start, int count) => _values.RemoveRange(start, count);

        public void Update(int index, string value) => _values[index] = value;
    }

    internal sealed class SizedStringValueSerializer : IBplusTreeValueSerializer<string, SizedStringValueContainer>
    {
        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context) => Task.CompletedTask;

        public SizedStringValueContainer CreateEmpty() => new SizedStringValueContainer();

        public SizedStringValueContainer Deserialize(ref SequenceReader<byte> reader)
        {
            var container = new SizedStringValueContainer();
            if (!reader.TryReadLittleEndian(out int count))
            {
                throw new InvalidOperationException("Failed to read count");
            }
            for (int i = 0; i < count; i++)
            {
                if (!reader.TryReadLittleEndian(out int length))
                {
                    throw new InvalidOperationException("Failed to read length");
                }
                var bytes = new byte[length];
                if (!reader.TryCopyTo(bytes))
                {
                    throw new InvalidOperationException("Failed to read value");
                }
                reader.Advance(length);
                container._values.Add(Encoding.ASCII.GetString(bytes));
            }
            return container;
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context) => Task.CompletedTask;

        // A count, then each value as its length and ASCII bytes, the sizes GetByteSize reports
        public void Serialize(in IBufferWriter<byte> writer, in SizedStringValueContainer values)
        {
            var countSpan = writer.GetSpan(sizeof(int));
            BinaryPrimitives.WriteInt32LittleEndian(countSpan, values.Count);
            writer.Advance(sizeof(int));
            foreach (var value in values._values)
            {
                var size = SizedStringValueContainer.EntrySize(value);
                var span = writer.GetSpan(size);
                BinaryPrimitives.WriteInt32LittleEndian(span, value.Length);
                Encoding.ASCII.GetBytes(value, span.Slice(sizeof(int)));
                writer.Advance(size);
            }
        }
    }

    /// <summary>
    /// A theory that reports Skipped off Linux, where the direct I/O writer does not exist.
    /// </summary>
    public sealed class LinuxOnlyTheoryAttribute : TheoryAttribute
    {
        public LinuxOnlyTheoryAttribute()
        {
            if (!OperatingSystem.IsLinux())
            {
                Skip = "The direct I/O writer only exists on Linux";
            }
        }
    }

    /// <summary>
    /// A fact that reports Skipped off Linux, where the direct I/O writer does not exist.
    /// </summary>
    public sealed class LinuxOnlyFactAttribute : FactAttribute
    {
        public LinuxOnlyFactAttribute()
        {
            if (!OperatingSystem.IsLinux())
            {
                Skip = "The direct I/O writer only exists on Linux";
            }
        }
    }
}
