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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using System.Collections.Immutable;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta
{
    internal class DeltaTable
    {
        private DeltaMetadataAction _metadata;
        private DeltaProtocolAction _protocol;
        private List<DeltaAddAction> _addFiles;
        private List<DeltaFile>? _files;
        private long _version;
        private StructType _schema;
        private readonly IReadOnlyCollection<DeltaRemoveFileAction> _tombstones;
        private readonly IReadOnlyDictionary<string, DeltaTransactionAction> _transactions;
        private readonly IReadOnlyDictionary<string, DeltaDomainMetadataAction> _domainMetadata;
        private readonly long? _startCheckpointVersion;

        internal DeltaTable(
            DeltaMetadataAction metadata,
            DeltaProtocolAction protocol,
            List<DeltaAddAction> addFiles,
            StructType schema,
            long version,
            IReadOnlyCollection<DeltaRemoveFileAction> tombstones,
            IReadOnlyDictionary<string, DeltaTransactionAction> transactions,
            IReadOnlyDictionary<string, DeltaDomainMetadataAction> domainMetadata,
            long? startCheckpointVersion)
        {
            _metadata = metadata;
            _protocol = protocol;
            _addFiles = addFiles;
            _version = version;
            _schema = schema;
            _tombstones = tombstones;
            _transactions = transactions;
            _domainMetadata = domainMetadata;
            _startCheckpointVersion = startCheckpointVersion;
        }

        public List<DeltaAddAction> AddFiles => _addFiles;

        /// <summary>
        /// The add actions with parsed statistics, parsed from <see cref="AddFiles"/> on first access.
        /// </summary>
        public List<DeltaFile> Files => _files ??= ParseFiles();

        public StructType Schema => _schema;

        internal DeltaMetadataAction Metadata => _metadata;

        internal DeltaProtocolAction Protocol => _protocol;

        public IReadOnlyList<string> PartitionColumns => _metadata.PartitionColumns ?? (IReadOnlyList<string>)ImmutableList<string>.Empty;

        public DeltaMetadataFormat Format => _metadata.Format ?? throw new Exception("Format must be defined");

        public long Version => _version;

        /// <summary>
        /// Remove actions that are still tombstones, empty when the table was read with <see cref="DeltaReadOptions.SkipTombstones"/>.
        /// </summary>
        public IReadOnlyCollection<DeltaRemoveFileAction> Tombstones => _tombstones;

        /// <summary>
        /// The latest txn action per application id.
        /// </summary>
        public IReadOnlyDictionary<string, DeltaTransactionAction> Transactions => _transactions;

        /// <summary>
        /// The latest domain metadata per domain, without removed domains.
        /// </summary>
        public IReadOnlyDictionary<string, DeltaDomainMetadataAction> DomainMetadata => _domainMetadata;

        /// <summary>
        /// The classic checkpoint the snapshot was read from, null when it was replayed from the first commit.
        /// </summary>
        public long? StartCheckpointVersion => _startCheckpointVersion;

        private List<DeltaFile> ParseFiles()
        {
            var statisticsJsonOptions = new JsonSerializerOptions();
            statisticsJsonOptions.Converters.Add(new DeltaStatisticsConverter(_schema));

            var files = new List<DeltaFile>(_addFiles.Count);
            foreach (var addFile in _addFiles)
            {
                if (addFile.Statistics != null)
                {
                    var stats = JsonSerializer.Deserialize<DeltaStatistics>(addFile.Statistics, statisticsJsonOptions);
                    files.Add(new DeltaFile(addFile, stats!));
                }
                else
                {
                    // Empty statistics is fine - just means no stats were collected
                    files.Add(new DeltaFile(addFile, new DeltaStatistics()));
                }
            }
            return files;
        }

        public bool DeleteVectorEnabled
        {
            get
            {
                bool featureEnabled = _protocol?.WriterFeatures?.Contains("deletionVectors") ?? false;
                if (!featureEnabled)
                {
                    return false;
                }
                if (_metadata.Configuration?.TryGetValue("delta.enableDeletionVectors", out var value) ?? false)
                {
                    if (value.Equals("true", StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
                return false;
            }
        }

        public bool ChangeDataEnabled
        {
            get
            {
                bool featureEnabled = _protocol?.WriterFeatures?.Contains("changeDataFeed") ?? false;
                if (!featureEnabled)
                {
                    return false;
                }
                if (_metadata.Configuration?.TryGetValue("delta.enableChangeDataFeed", out var value) ?? false)
                {
                    if (value.Equals("true", StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
                return false;
            }
        }

        public bool ColumnMappingEnabled
        {
            get
            {
                if (_metadata.Configuration != null &&
                    _metadata.Configuration.TryGetValue("delta.columnMapping.mode", out var mode))
                {
                    return !string.Equals(mode, "none", StringComparison.OrdinalIgnoreCase);
                }
                return false;
            }
        }

        public int? MaxColumnId
        {
            get
            {
                if (_metadata.Configuration != null &&
                    _metadata.Configuration.TryGetValue("delta.columnMapping.maxColumnId", out var maxColumnIdStr) &&
                        int.TryParse(maxColumnIdStr, out var maxColumnId))
                {
                    return maxColumnId;
                }
                return default;
            }
            set
            {
                if (_metadata.Configuration == null)
                {
                    _metadata.Configuration = new Dictionary<string, string>();
                }
                if (value.HasValue)
                {
                    _metadata.Configuration["delta.columnMapping.maxColumnId"] = value.Value.ToString();
                }
                else
                {
                    _metadata.Configuration.Remove("delta.columnMapping.maxColumnId");
                }
            }
        }
    }
}
