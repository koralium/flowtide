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

using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.StateManager.Internal;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json;

namespace FlowtideDotNet.Benchmarks.LateMaterialization
{
    internal sealed class LmCensusRow
    {
        public required string Operator { get; init; }

        public required string DisplayName { get; init; }

        public required string Tree { get; init; }

        public required string StateClass { get; init; }

        public required long Bytes { get; init; }

        public required int Pages { get; init; }
    }

    /// <summary>
    /// Wraps a storage and counts the live bytes of every state tree.
    /// </summary>
    internal sealed class LmCensusStorage : IPersistentStorage
    {
        private readonly ReservoirPersistentStorage _inner;
        private readonly List<LmCensusSession> _sessions = new List<LmCensusSession>();
        private readonly object _lock = new object();
        private readonly Dictionary<long, int> _storagePages = new Dictionary<long, int>();
        private Dictionary<string, long> _clientMetadataPages = new Dictionary<string, long>();
        private int _managerMetadataBytes;

        public LmCensusStorage(ReservoirPersistentStorage inner)
        {
            _inner = inner;
        }

        public bool SupportsDistributedCheckpoints => _inner.SupportsDistributedCheckpoints;

        public long CurrentVersion => _inner.CurrentVersion;

        public Task InitializeAsync(StorageInitializationMetadata metadata)
        {
            return _inner.InitializeAsync(metadata);
        }

        public IPersistentStorageSession CreateSession()
        {
            var session = new LmCensusSession(_inner.CreateSession());
            lock (_lock)
            {
                _sessions.Add(session);
            }
            return session;
        }

        public ValueTask CheckpointAsync(byte[] metadata, bool includeIndex)
        {
            // The manager metadata names every tree's metadata page
            using var document = JsonDocument.Parse(metadata);
            var pages = new Dictionary<string, long>();
            if (document.RootElement.TryGetProperty("ClientMetadataLocations", out var locations))
            {
                foreach (var location in locations.EnumerateObject())
                {
                    pages[location.Name] = location.Value.GetInt64();
                }
            }
            lock (_lock)
            {
                _clientMetadataPages = pages;
                // The storage keeps this metadata as a page of its own
                _managerMetadataBytes = metadata.Length;
            }
            return _inner.CheckpointAsync(metadata, includeIndex);
        }

        public ValueTask CompactAsync(ulong changesSinceLastCompact, ulong pageCount)
        {
            return _inner.CompactAsync(changesSinceLastCompact, pageCount);
        }

        public ValueTask ResetAsync()
        {
            Forget();
            return _inner.ResetAsync();
        }

        public ValueTask RecoverAsync(long checkpointVersion)
        {
            // The counts no longer match what the storage holds
            Forget();
            return _inner.RecoverAsync(checkpointVersion);
        }

        public bool TryGetValue(long key, [NotNullWhen(true)] out ReadOnlyMemory<byte>? value)
        {
            return _inner.TryGetValue(key, out value);
        }

        public ValueTask Write(long key, byte[] value)
        {
            lock (_lock)
            {
                _storagePages[key] = value.Length;
            }
            return _inner.Write(key, value);
        }

        public void ClearForRestore()
        {
            Forget();
            _inner.ClearForRestore();
        }

        private void Forget()
        {
            lock (_lock)
            {
                _storagePages.Clear();
                _managerMetadataBytes = 0;
                for (int i = 0; i < _sessions.Count; i++)
                {
                    _sessions[i].Forget();
                }
            }
        }

        public void Dispose()
        {
            _inner.Dispose();
        }

        /// <summary>
        /// Gives the live bytes per tree, named from the last checkpoint.
        /// </summary>
        public List<LmCensusRow> Snapshot(StreamGraph graph)
        {
            var rows = new List<LmCensusRow>();
            Dictionary<string, long> pages;
            List<LmCensusSession> sessions;
            lock (_lock)
            {
                pages = _clientMetadataPages;
                sessions = new List<LmCensusSession>(_sessions);
            }
            var named = new HashSet<LmCensusSession>();
            foreach (var client in pages)
            {
                for (int i = 0; i < sessions.Count; i++)
                {
                    if (!sessions[i].HasPage(client.Value))
                    {
                        continue;
                    }
                    named.Add(sessions[i]);
                    int split = client.Key.IndexOf('_');
                    string operatorName = split < 0 ? client.Key : client.Key.Substring(0, split);
                    string tree = split < 0 ? string.Empty : client.Key.Substring(split + 1);
                    string displayName = graph.Nodes.TryGetValue(operatorName, out var node) ? node.DisplayName : "unknown";
                    bool joinAbove = LmCensus.HasJoinAbove(graph, operatorName);
                    var (bytes, count) = sessions[i].Live();
                    rows.Add(new LmCensusRow()
                    {
                        Operator = operatorName,
                        DisplayName = displayName,
                        Tree = tree,
                        StateClass = LmCensus.Classify(displayName, tree, joinAbove),
                        Bytes = bytes,
                        Pages = count
                    });
                    break;
                }
            }
            // Sessions without a named tree are still counted
            long unnamedBytes = 0;
            int unnamedPages = 0;
            for (int i = 0; i < sessions.Count; i++)
            {
                if (!named.Contains(sessions[i]))
                {
                    var (bytes, count) = sessions[i].Live();
                    unnamedBytes += bytes;
                    unnamedPages += count;
                }
            }
            rows.Add(new LmCensusRow()
            {
                Operator = string.Empty,
                DisplayName = "unattributed",
                Tree = string.Empty,
                StateClass = LmCensus.Unattributed,
                Bytes = unnamedBytes,
                Pages = unnamedPages
            });
            long metadataBytes;
            int metadataPages;
            lock (_lock)
            {
                metadataBytes = _managerMetadataBytes;
                metadataPages = _managerMetadataBytes > 0 ? 1 : 0;
                foreach (var page in _storagePages)
                {
                    metadataBytes += page.Value;
                    metadataPages++;
                }
            }
            rows.Add(new LmCensusRow()
            {
                Operator = string.Empty,
                DisplayName = "stream metadata",
                Tree = string.Empty,
                StateClass = LmCensus.Metadata,
                Bytes = metadataBytes,
                Pages = metadataPages
            });
            return rows;
        }

        /// <summary>
        /// Compares counted pages with the storage's page map, both ways.
        /// Only valid right after a checkpoint.
        /// </summary>
        public List<string> Verify()
        {
            var problems = new List<string>();
            // The storage keeps no public list of its live pages
            var handler = typeof(ReservoirPersistentStorage)
                .GetField("_checkpointHandler", BindingFlags.NonPublic | BindingFlags.Instance)?
                .GetValue(_inner) as CheckpointHandler;
            if (handler == null)
            {
                problems.Add("the storage's page map could not be read, the census is not checked");
                return problems;
            }
            var counted = new Dictionary<long, int>();
            lock (_lock)
            {
                for (int i = 0; i < _sessions.Count; i++)
                {
                    foreach (var page in _sessions[i].Pages())
                    {
                        if (!counted.TryAdd(page.Key, page.Value))
                        {
                            problems.Add($"page {page.Key} is counted by two trees");
                        }
                    }
                }
                foreach (var page in _storagePages)
                {
                    if (!counted.TryAdd(page.Key, page.Value))
                    {
                        problems.Add($"page {page.Key} is counted as a tree's page and as a storage page");
                    }
                }
                // The storage writes the manager metadata as page one
                if (_managerMetadataBytes > 0 && !counted.TryAdd(1, _managerMetadataBytes))
                {
                    problems.Add("page 1 is counted twice");
                }
            }
            var stored = handler.PageFileLocations_Test;
            long storedBytes = 0;
            foreach (var page in stored)
            {
                storedBytes += page.Value.Size;
                if (!counted.ContainsKey(page.Key) && problems.Count < 10)
                {
                    problems.Add($"the storage holds page {page.Key} of {page.Value.Size} bytes, the census does not count it");
                }
            }
            long countedBytes = 0;
            foreach (var page in counted)
            {
                countedBytes += page.Value;
                if (problems.Count >= 10)
                {
                    continue;
                }
                if (!stored.TryGetValue(page.Key, out var location))
                {
                    problems.Add($"page {page.Key} is counted but the storage does not hold it");
                }
                else if (location.Size != page.Value)
                {
                    problems.Add($"page {page.Key} is counted as {page.Value} bytes, the storage holds {location.Size}");
                }
            }
            if (storedBytes != countedBytes)
            {
                problems.Add($"the storage holds {storedBytes} bytes in {stored.Count} pages, the census counts {countedBytes} bytes in {counted.Count}");
            }
            return problems;
        }

        public long TotalLiveBytes()
        {
            long total = 0;
            lock (_lock)
            {
                total += _managerMetadataBytes;
                foreach (var page in _storagePages)
                {
                    total += page.Value;
                }
                for (int i = 0; i < _sessions.Count; i++)
                {
                    total += _sessions[i].Live().bytes;
                }
            }
            return total;
        }
    }

    internal sealed class LmCensusSession : IPersistentStorageSession
    {
        private readonly IPersistentStorageSession _inner;
        private readonly Dictionary<long, int> _pages = new Dictionary<long, int>();
        private readonly LmCountingWriter _sizer = new LmCountingWriter();
        private readonly object _lock = new object();

        public LmCensusSession(IPersistentStorageSession inner)
        {
            _inner = inner;
        }

        public bool SupportsConcurrentReads => _inner.SupportsConcurrentReads;

        public ValueTask<T> Read<T>(long key, IStateSerializer<T> stateSerializer)
            where T : ICacheObject
        {
            return _inner.Read(key, stateSerializer);
        }

        public ValueTask<ReadOnlyMemory<byte>> Read(long key)
        {
            return _inner.Read(key);
        }

        public Task Write(long key, SerializableObject value)
        {
            lock (_lock)
            {
                int size;
                if (value.HasPreSerializedData)
                {
                    size = value.PreSerializedData!.Value.Length;
                }
                else
                {
                    // Serialize once more only to learn the size
                    _sizer.Reset();
                    value.Serialize(_sizer);
                    size = _sizer.Written;
                }
                _pages[key] = size;
            }
            return _inner.Write(key, value);
        }

        public Task Delete(long key)
        {
            lock (_lock)
            {
                _pages.Remove(key);
            }
            return _inner.Delete(key);
        }

        public Task Commit()
        {
            return _inner.Commit();
        }

        public void Dispose()
        {
            _inner.Dispose();
        }

        public void Forget()
        {
            lock (_lock)
            {
                _pages.Clear();
            }
        }

        public List<KeyValuePair<long, int>> Pages()
        {
            lock (_lock)
            {
                return new List<KeyValuePair<long, int>>(_pages);
            }
        }

        public bool HasPage(long key)
        {
            lock (_lock)
            {
                return _pages.ContainsKey(key);
            }
        }

        public (long bytes, int pages) Live()
        {
            lock (_lock)
            {
                long bytes = 0;
                foreach (var page in _pages)
                {
                    bytes += page.Value;
                }
                return (bytes, _pages.Count);
            }
        }
    }

    /// <summary>
    /// Counts written bytes without keeping them.
    /// </summary>
    internal sealed class LmCountingWriter : IBufferWriter<byte>
    {
        private byte[] _buffer = new byte[64 * 1024];

        public int Written { get; private set; }

        public void Reset()
        {
            Written = 0;
        }

        public void Advance(int count)
        {
            Written += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            Grow(sizeHint);
            return _buffer;
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            Grow(sizeHint);
            return _buffer;
        }

        private void Grow(int sizeHint)
        {
            if (sizeHint > _buffer.Length)
            {
                _buffer = new byte[sizeHint];
            }
        }
    }

    internal static class LmCensus
    {
        public const string SourceDisplayName = "LM Source";
        public const string SinkDisplayName = "LM Sink";

        public const string Source = "source";
        public const string JoinInput = "join input";
        public const string JoinIntermediate = "join intermediate";
        public const string JoinIndex = "join index";
        public const string JoinOther = "join other";
        public const string AggregateMain = "aggregate main";
        public const string AggregateMeasure = "aggregate measure";
        public const string AggregateOther = "aggregate other";
        public const string Sink = "sink";
        public const string Other = "other";
        public const string Metadata = "stream metadata";
        public const string Unattributed = "unattributed";

        public static readonly string[] Classes = [Source, JoinInput, JoinIntermediate, JoinIndex, JoinOther, AggregateMain, AggregateMeasure, AggregateOther, Sink, Other, Metadata, Unattributed];

        // Operators that keep no rows
        private static readonly string[] Stateless = ["Projection", "Filter", "Exchange", "Partition", "Partitioned Output", "Set(UnionAll)"];

        /// <summary>
        /// True when a join feeds this operator, directly or through stateless operators.
        /// </summary>
        public static bool HasJoinAbove(StreamGraph graph, string operatorName)
        {
            var pending = new Queue<string>();
            var seen = new HashSet<string>();
            pending.Enqueue(operatorName);
            while (pending.Count > 0)
            {
                var current = pending.Dequeue();
                for (int i = 0; i < graph.Edges.Count; i++)
                {
                    var edge = graph.Edges[i];
                    if (edge.Target != current || !seen.Add(edge.Source))
                    {
                        continue;
                    }
                    if (!graph.Nodes.TryGetValue(edge.Source, out var node))
                    {
                        continue;
                    }
                    if (IsJoin(node.DisplayName))
                    {
                        return true;
                    }
                    if (Array.IndexOf(Stateless, node.DisplayName) >= 0)
                    {
                        pending.Enqueue(edge.Source);
                    }
                }
            }
            return false;
        }

        private static bool IsJoin(string displayName)
        {
            return displayName == "Multi Join" || displayName == "Merge Join Bulk";
        }

        public static string Classify(string displayName, string tree, bool joinAbove)
        {
            if (displayName.StartsWith(SourceDisplayName, StringComparison.Ordinal))
            {
                return Source;
            }
            if (displayName == SinkDisplayName)
            {
                return Sink;
            }
            if (displayName == "Multi Join")
            {
                // An input's rows are in tree p and its index
                if (tree.Length > 1 && tree[0] == 's' && char.IsDigit(tree[1]))
                {
                    return JoinIndex;
                }
                if (tree.Length < 2 || tree[0] != 'p' || !char.IsDigit(tree[1]))
                {
                    return JoinOther;
                }
                // A join above feeds the first input in these plans
                return joinAbove && tree == "p0" ? JoinIntermediate : JoinInput;
            }
            if (displayName == "Merge Join Bulk")
            {
                if (tree == "left" && joinAbove)
                {
                    return JoinIntermediate;
                }
                return tree == "left" || tree == "right" ? JoinInput : JoinOther;
            }
            if (displayName == "Aggregation")
            {
                if (tree == "grouping_set_1_v1")
                {
                    return AggregateMain;
                }
                // The temporary tree and the sent flag hold no measure values
                return tree == "grouping_set_1_v1_temp" || tree == "initialDataSent" ? AggregateOther : AggregateMeasure;
            }
            return Other;
        }

        public static Dictionary<string, long> ByClass(List<LmCensusRow> rows)
        {
            var result = new Dictionary<string, long>();
            for (int i = 0; i < Classes.Length; i++)
            {
                result[Classes[i]] = 0;
            }
            for (int i = 0; i < rows.Count; i++)
            {
                result[rows[i].StateClass] += rows[i].Bytes;
            }
            return result;
        }
    }
}
