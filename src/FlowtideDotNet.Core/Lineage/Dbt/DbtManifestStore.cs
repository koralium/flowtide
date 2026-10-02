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

using FlowtideDotNet.Core.Lineage.Dbt.Internal;
using FlowtideDotNet.Core.Lineage.Internal.Models;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Lineage.Dbt
{
    /// <summary>
    /// Serves stream lineage as a mock dbt manifest.
    /// </summary>
    public sealed class DbtManifestStore
    {
        private static readonly Comparer<(string Stream, string Substream)> s_keyComparer = Comparer<(string Stream, string Substream)>.Create((x, y) =>
        {
            var result = string.CompareOrdinal(x.Stream, y.Stream);
            return result != 0 ? result : string.CompareOrdinal(x.Substream, y.Substream);
        });

        private readonly DbtGeneratorSettings _settings;
        private readonly TimeProvider _timeProvider;
        private readonly long _createdTimestamp;
        private readonly object _lock = new object();
        private volatile StoreState _state = new StoreState(0, ImmutableSortedDictionary.Create<(string Stream, string Substream), DbtRegistration>(s_keyComparer));
        private volatile ImmutableHashSet<string> _expectedStreams = ImmutableHashSet.Create<string>(StringComparer.Ordinal);

        /// <summary>
        /// Creates a store with default options.
        /// </summary>
        public DbtManifestStore()
            : this(new DbtManifestOptions())
        {
        }

        /// <summary>
        /// Creates a store from a frozen options copy.
        /// </summary>
        /// <param name="options">Options copied at construction.</param>
        public DbtManifestStore(DbtManifestOptions options)
            : this(options, TimeProvider.System)
        {
        }

        internal DbtManifestStore(DbtManifestOptions options, TimeProvider timeProvider)
        {
            _settings = DbtGeneratorSettings.Create(options);
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
        /// Manifest for every registered stream.
        /// </summary>
        /// <returns>The manifest, empty when nothing is registered.</returns>
        public DbtArtifact GetManifest()
        {
            return GetCombined(_state).Manifest;
        }

        /// <summary>
        /// Catalog for every registered stream.
        /// </summary>
        /// <returns>The catalog, empty when nothing is registered.</returns>
        public DbtArtifact GetCatalog()
        {
            return GetCombined(_state).Catalog;
        }

        /// <summary>
        /// Manifest for one logical stream and its substreams.
        /// </summary>
        /// <param name="streamName">Logical stream name, case sensitive.</param>
        /// <param name="manifest">The manifest when the stream is known.</param>
        /// <returns>False when the stream is not registered.</returns>
        public bool TryGetManifest(string streamName, [NotNullWhen(true)] out DbtArtifact? manifest)
        {
            manifest = TryGetStream(_state, streamName)?.Manifest;
            return manifest != null;
        }

        /// <summary>
        /// Catalog for one logical stream and its substreams.
        /// </summary>
        /// <param name="streamName">Logical stream name, case sensitive.</param>
        /// <param name="catalog">The catalog when the stream is known.</param>
        /// <returns>False when the stream is not registered.</returns>
        public bool TryGetCatalog(string streamName, [NotNullWhen(true)] out DbtArtifact? catalog)
        {
            catalog = TryGetStream(_state, streamName)?.Catalog;
            return catalog != null;
        }

        // Last registration per (stream, substream) wins.
        internal void Register(StreamLineage lineage, string logicalStreamName)
        {
            ArgumentNullException.ThrowIfNull(lineage);
            ArgumentNullException.ThrowIfNull(logicalStreamName);
            lock (_lock)
            {
                var state = _state;
                var entries = state.Entries.SetItem((logicalStreamName, lineage.SubstreamName ?? string.Empty), new DbtRegistration(logicalStreamName, lineage));
                _state = new StoreState(state.Version + 1, entries);
            }
        }

        private DbtScopeArtifacts GetCombined(StoreState state)
        {
            var cached = state.Combined;
            if (cached != null)
            {
                return cached;
            }
            // Failures propagate and are never cached.
            var generated = DbtScopeArtifacts.Generate(state.Entries.Values.ToList(), _settings);
            return Interlocked.CompareExchange(ref state.Combined, generated, null) ?? generated;
        }

        private DbtScopeArtifacts? TryGetStream(StoreState state, string streamName)
        {
            ArgumentNullException.ThrowIfNull(streamName);
            if (!state.StreamNames.Contains(streamName))
            {
                return null;
            }
            if (state.Streams.TryGetValue(streamName, out var cached))
            {
                return cached;
            }
            var registrations = state.Entries.Values.Where(x => string.Equals(x.StreamName, streamName, StringComparison.Ordinal)).ToList();
            return state.Streams.GetOrAdd(streamName, DbtScopeArtifacts.Generate(registrations, _settings));
        }

        // Immutable apart from caches, swapped whole on register.
        private sealed class StoreState
        {
            public DbtScopeArtifacts? Combined;

            public StoreState(long version, ImmutableSortedDictionary<(string Stream, string Substream), DbtRegistration> entries)
            {
                Version = version;
                Entries = entries;
                StreamNames = entries.Keys.Select(x => x.Stream).ToImmutableHashSet(StringComparer.Ordinal);
            }

            public long Version { get; }

            public ImmutableSortedDictionary<(string Stream, string Substream), DbtRegistration> Entries { get; }

            public ImmutableHashSet<string> StreamNames { get; }

            public ConcurrentDictionary<string, DbtScopeArtifacts> Streams { get; } = new ConcurrentDictionary<string, DbtScopeArtifacts>(StringComparer.Ordinal);
        }
    }
}
