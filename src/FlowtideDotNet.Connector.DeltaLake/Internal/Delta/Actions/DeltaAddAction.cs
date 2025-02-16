using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions
{
    internal class DeltaAddAction : DeltaBaseAction
    {
        [JsonPropertyName("path")]
        public string? Path { get; set; }

        [JsonPropertyName("partitionValues")]
        public Dictionary<string, string>? PartitionValues { get; set; }

        [JsonPropertyName("size")]
        public long Size { get; set; }

        [JsonPropertyName("modificationTime")]
        public long ModificationTime { get; set; }

        [JsonPropertyName("dataChange")]
        public bool DataChange { get; set; }

        [JsonPropertyName("stats")]
        public string? Statistics { get; set; }

        [JsonPropertyName("tags")]
        public Dictionary<string, string>? Tags { get; set; }

        [JsonPropertyName("baseRowId")]
        public long? BaseRowId { get; set; }

        [JsonPropertyName("defaultRowCommitVersion")]
        public long? DefaultRowCommitVersion { get; set; }

        [JsonPropertyName("clusteringProvider")]
        public string? ClusteringProvider { get; set; }

        [JsonPropertyName("deletionVector")]
        public DeletionVector? DeletionVector { get; set; }

        public DeltaFileKey GetKey()
        {
            return new DeltaFileKey(Path!, DeletionVector?.UniqueId);
        }
    }
}
