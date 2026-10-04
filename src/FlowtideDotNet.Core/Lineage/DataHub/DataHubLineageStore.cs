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

using FlowtideDotNet.Core.Lineage.DataHub.Internal;
using FlowtideDotNet.Core.Lineage.Internal.Models;
using System.Collections.Immutable;

namespace FlowtideDotNet.Core.Lineage.DataHub
{
    /// <summary>
    /// Serves stream lineage as DataHub entities.
    /// </summary>
    public sealed class DataHubLineageStore
    {
        private static readonly Comparer<(string Stream, string Substream)> s_keyComparer = Comparer<(string Stream, string Substream)>.Create((x, y) =>
        {
            var result = string.CompareOrdinal(x.Stream, y.Stream);
            return result != 0 ? result : string.CompareOrdinal(x.Substream, y.Substream);
        });

        private readonly DataHubSettings _settings;
        private readonly TimeProvider _timeProvider;
        private readonly long _createdTimestamp;
        private readonly object _lock = new object();
        private volatile StoreState _state = new StoreState(0, ImmutableSortedDictionary.Create<(string Stream, string Substream), DataHubRegistration>(s_keyComparer));
        private volatile ImmutableHashSet<string> _expectedStreams = ImmutableHashSet.Create<string>(StringComparer.Ordinal);

        /// <summary>
        /// Creates a store with default options.
        /// </summary>
        public DataHubLineageStore()
            : this(new DataHubLineageOptions())
        {
        }

        /// <summary>
        /// Creates a store from a frozen options copy.
        /// </summary>
        /// <param name="options">Options copied at construction.</param>
        public DataHubLineageStore(DataHubLineageOptions options)
            : this(options, TimeProvider.System)
        {
        }

        internal DataHubLineageStore(DataHubLineageOptions options, TimeProvider timeProvider)
        {
            _settings = DataHubSettings.Create(options);
            _timeProvider = timeProvider;
            _createdTimestamp = timeProvider.GetTimestamp();
        }

        /// <summary>
        /// True while expected streams are missing, within the timeout.
        /// </summary>
        public bool IsWarmingUp
        {
            get
            {
                var expected = _expectedStreams;
                if (expected.IsEmpty || _timeProvider.GetElapsedTime(_createdTimestamp) >= _settings.WarmupTimeout)
                {
                    return false;
                }
                var state = _state;
                return expected.Any(x => !state.StreamNames.Contains(x));
            }
        }

        internal bool IncludeConnectorSchema => _settings.IncludeConnectorSchema;

        internal long Version => _state.Version;

        /// <summary>
        /// Expects a stream, warming up until it registers.
        /// </summary>
        /// <param name="streamName">Logical stream name expected to register.</param>
        public void ExpectStream(string streamName)
        {
            ArgumentException.ThrowIfNullOrEmpty(streamName);
            lock (_lock)
            {
                _expectedStreams = _expectedStreams.Add(streamName);
            }
        }

        /// <summary>
        /// Entities of every registered stream.
        /// </summary>
        /// <returns>The snapshot, empty when nothing is registered.</returns>
        public DataHubSnapshot GetSnapshot()
        {
            var state = _state;
            var cached = state.Snapshot;
            if (cached != null)
            {
                return cached;
            }
            // Concurrent requests share one generation, failures are never cached.
            lock (state)
            {
                cached = state.Snapshot;
                if (cached != null)
                {
                    return cached;
                }
                var generated = DataHubEntityGenerator.Generate(state.Entries.Values.ToList(), _settings);
                state.Snapshot = generated;
                return generated;
            }
        }

        // Last registration per (stream, substream) wins.
        internal void Register(StreamLineage lineage, string logicalStreamName)
        {
            ArgumentNullException.ThrowIfNull(lineage);
            ArgumentNullException.ThrowIfNull(logicalStreamName);
            lock (_lock)
            {
                var state = _state;
                var entries = state.Entries.SetItem((logicalStreamName, lineage.SubstreamName ?? string.Empty), new DataHubRegistration(logicalStreamName, lineage));
                _state = new StoreState(state.Version + 1, entries);
            }
        }

        // Immutable apart from the cache, swapped whole on register.
        private sealed class StoreState
        {
            public volatile DataHubSnapshot? Snapshot;

            public StoreState(long version, ImmutableSortedDictionary<(string Stream, string Substream), DataHubRegistration> entries)
            {
                Version = version;
                Entries = entries;
                StreamNames = entries.Keys.Select(x => x.Stream).ToImmutableHashSet(StringComparer.Ordinal);
            }

            public long Version { get; }

            public ImmutableSortedDictionary<(string Stream, string Substream), DataHubRegistration> Entries { get; }

            public ImmutableHashSet<string> StreamNames { get; }
        }
    }
}
