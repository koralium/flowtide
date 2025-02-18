using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors
{
    internal class DeletionVector
    {
        [JsonPropertyName("storageType")]
        public string? StorageType { get; set; }

        [JsonPropertyName("pathOrInlineDv")]
        public string? PathOrInlineDv { get; set; }

        [JsonPropertyName("offset")]
        public long? Offset { get; set; }

        [JsonPropertyName("sizeInBytes")]
        public int SizeInBytes { get; set; }

        [JsonPropertyName("cardinality")]
        public long Cardinality { get; set; }

        [JsonIgnore]
        public string UniqueId => GetUniqueId();

        [JsonIgnore]
        public string? AbsolutePath => GetAbsolutePath();

        private string GetUniqueId()
        {
            if (Offset == null)
            {
                return $"{StorageType}{PathOrInlineDv}";
            }
            return $"{StorageType}{PathOrInlineDv}@{Offset}";
        }

        public string? GetAbsolutePath()
        {
            if (StorageType == "p")
            {
                return PathOrInlineDv!;
            }
            if (StorageType == "u")
            {
                var guid = Z85Helper.DecodeToGuid(PathOrInlineDv!);
                var deletionVectorPath = "deletion_vector_" + guid.ToString() + ".bin";
                return deletionVectorPath;
            }
            return null;
        }
    }
}
