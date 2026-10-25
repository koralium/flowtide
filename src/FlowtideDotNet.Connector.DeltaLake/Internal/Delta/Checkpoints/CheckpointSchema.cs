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

using Apache.Arrow;
using Apache.Arrow.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// The 52 leaves of a checkpoint, in schema order.
    /// </summary>
    internal enum Leaf : byte
    {
        TxnAppId, TxnVersion, TxnLastUpdated,
        AddPath, AddPartitionKey, AddPartitionValue, AddSize, AddModificationTime, AddDataChange, AddStats, AddTagKey, AddTagValue,
        AddDvStorageType, AddDvPath, AddDvOffset, AddDvSize, AddDvCardinality, AddBaseRowId, AddDefaultRowCommitVersion, AddClusteringProvider,
        RemovePath, RemoveDeletionTimestamp, RemoveDataChange, RemoveExtendedFileMetadata, RemovePartitionKey, RemovePartitionValue, RemoveSize,
        RemoveDvStorageType, RemoveDvPath, RemoveDvOffset, RemoveDvSize, RemoveDvCardinality, RemoveBaseRowId, RemoveDefaultRowCommitVersion,
        MetaId, MetaName, MetaDescription, MetaFormatProvider, MetaFormatOptionKey, MetaFormatOptionValue, MetaSchemaString, MetaPartitionColumn,
        MetaCreatedTime, MetaConfigurationKey, MetaConfigurationValue,
        ProtocolMinReader, ProtocolMinWriter, ProtocolReaderFeature, ProtocolWriterFeature,
        DomainName, DomainConfiguration, DomainRemoved
    }

    /// <summary>
    /// A map or list in the schema; its leaves repeat once per entry.
    /// </summary>
    internal enum RepeatedNode : byte
    {
        AddPartitionValues, AddTags, RemovePartitionValues, MetaFormatOptions, MetaPartitionColumns, MetaConfiguration, ProtocolReaderFeatures, ProtocolWriterFeatures
    }

    internal enum LeafType : byte
    {
        String,
        Int64,
        Int32,
        Bool
    }

    /// <summary>
    /// The static schema our checkpoints use: txn, add, remove, metaData, protocol and domainMetadata, every field nullable.
    /// </summary>
    internal static class CheckpointSchema
    {
        public const int LeafCount = 52;
        public const int RepeatedCount = 8;

        private static readonly LeafType[] s_types = BuildTypes();
        private static readonly int[] s_repeated = BuildRepeated();

        public static LeafType Type(Leaf leaf) => s_types[(int)leaf];

        // The repeated node a leaf belongs to, or -1
        public static int RepeatedOf(Leaf leaf) => s_repeated[(int)leaf];

        // Every leaf is optional, so it has definition levels, and repetition levels inside a map or list
        public static int LevelStreams(Leaf leaf) => s_repeated[(int)leaf] >= 0 ? 2 : 1;

        // Map keys are the only required leaves
        public static bool IsMapKey(Leaf leaf) => leaf is Leaf.AddPartitionKey or Leaf.AddTagKey or Leaf.RemovePartitionKey or Leaf.MetaFormatOptionKey or Leaf.MetaConfigurationKey;

        private static LeafType[] BuildTypes()
        {
            var types = new LeafType[LeafCount];
            foreach (var leaf in new[] { Leaf.TxnVersion, Leaf.TxnLastUpdated, Leaf.AddSize, Leaf.AddModificationTime, Leaf.AddDvCardinality, Leaf.AddBaseRowId,
                Leaf.AddDefaultRowCommitVersion, Leaf.RemoveDeletionTimestamp, Leaf.RemoveSize, Leaf.RemoveDvCardinality, Leaf.RemoveBaseRowId,
                Leaf.RemoveDefaultRowCommitVersion, Leaf.MetaCreatedTime })
            {
                types[(int)leaf] = LeafType.Int64;
            }
            foreach (var leaf in new[] { Leaf.AddDvOffset, Leaf.AddDvSize, Leaf.RemoveDvOffset, Leaf.RemoveDvSize, Leaf.ProtocolMinReader, Leaf.ProtocolMinWriter })
            {
                types[(int)leaf] = LeafType.Int32;
            }
            foreach (var leaf in new[] { Leaf.AddDataChange, Leaf.RemoveDataChange, Leaf.RemoveExtendedFileMetadata, Leaf.DomainRemoved })
            {
                types[(int)leaf] = LeafType.Bool;
            }
            return types;
        }

        private static int[] BuildRepeated()
        {
            var repeated = new int[LeafCount];
            System.Array.Fill(repeated, -1);
            void Set(RepeatedNode node, params Leaf[] leaves)
            {
                foreach (var leaf in leaves)
                {
                    repeated[(int)leaf] = (int)node;
                }
            }
            Set(RepeatedNode.AddPartitionValues, Leaf.AddPartitionKey, Leaf.AddPartitionValue);
            Set(RepeatedNode.AddTags, Leaf.AddTagKey, Leaf.AddTagValue);
            Set(RepeatedNode.RemovePartitionValues, Leaf.RemovePartitionKey, Leaf.RemovePartitionValue);
            Set(RepeatedNode.MetaFormatOptions, Leaf.MetaFormatOptionKey, Leaf.MetaFormatOptionValue);
            Set(RepeatedNode.MetaPartitionColumns, Leaf.MetaPartitionColumn);
            Set(RepeatedNode.MetaConfiguration, Leaf.MetaConfigurationKey, Leaf.MetaConfigurationValue);
            Set(RepeatedNode.ProtocolReaderFeatures, Leaf.ProtocolReaderFeature);
            Set(RepeatedNode.ProtocolWriterFeatures, Leaf.ProtocolWriterFeature);
            return repeated;
        }

        public static readonly MapType StringMap = new MapType(StringType.Default, StringType.Default, keySorted: false);

        public static readonly ListType StringList = new ListType(new Field("element", StringType.Default, true));

        public static readonly StructType DeletionVector = new StructType(new[]
        {
            new Field("storageType", StringType.Default, true),
            new Field("pathOrInlineDv", StringType.Default, true),
            new Field("offset", Int32Type.Default, true),
            new Field("sizeInBytes", Int32Type.Default, true),
            new Field("cardinality", Int64Type.Default, true)
        });

        public static readonly StructType Txn = new StructType(new[]
        {
            new Field("appId", StringType.Default, true),
            new Field("version", Int64Type.Default, true),
            new Field("lastUpdated", Int64Type.Default, true)
        });

        public static readonly StructType Add = new StructType(new[]
        {
            new Field("path", StringType.Default, true),
            new Field("partitionValues", StringMap, true),
            new Field("size", Int64Type.Default, true),
            new Field("modificationTime", Int64Type.Default, true),
            new Field("dataChange", BooleanType.Default, true),
            new Field("stats", StringType.Default, true),
            new Field("tags", StringMap, true),
            new Field("deletionVector", DeletionVector, true),
            new Field("baseRowId", Int64Type.Default, true),
            new Field("defaultRowCommitVersion", Int64Type.Default, true),
            new Field("clusteringProvider", StringType.Default, true)
        });

        public static readonly StructType Remove = new StructType(new[]
        {
            new Field("path", StringType.Default, true),
            new Field("deletionTimestamp", Int64Type.Default, true),
            new Field("dataChange", BooleanType.Default, true),
            new Field("extendedFileMetadata", BooleanType.Default, true),
            new Field("partitionValues", StringMap, true),
            new Field("size", Int64Type.Default, true),
            new Field("deletionVector", DeletionVector, true),
            new Field("baseRowId", Int64Type.Default, true),
            new Field("defaultRowCommitVersion", Int64Type.Default, true)
        });

        public static readonly StructType Format = new StructType(new[]
        {
            new Field("provider", StringType.Default, true),
            new Field("options", StringMap, true)
        });

        public static readonly StructType MetaData = new StructType(new[]
        {
            new Field("id", StringType.Default, true),
            new Field("name", StringType.Default, true),
            new Field("description", StringType.Default, true),
            new Field("format", Format, true),
            new Field("schemaString", StringType.Default, true),
            new Field("partitionColumns", StringList, true),
            new Field("createdTime", Int64Type.Default, true),
            new Field("configuration", StringMap, true)
        });

        public static readonly StructType Protocol = new StructType(new[]
        {
            new Field("minReaderVersion", Int32Type.Default, true),
            new Field("minWriterVersion", Int32Type.Default, true),
            new Field("readerFeatures", StringList, true),
            new Field("writerFeatures", StringList, true)
        });

        public static readonly StructType DomainMetadata = new StructType(new[]
        {
            new Field("domain", StringType.Default, true),
            new Field("configuration", StringType.Default, true),
            new Field("removed", BooleanType.Default, true)
        });

        public static Apache.Arrow.Schema Build(string? marker)
        {
            var builder = new Apache.Arrow.Schema.Builder()
                .Field(new Field("txn", Txn, true))
                .Field(new Field("add", Add, true))
                .Field(new Field("remove", Remove, true))
                .Field(new Field("metaData", MetaData, true))
                .Field(new Field("protocol", Protocol, true))
                .Field(new Field("domainMetadata", DomainMetadata, true));
            if (marker != null)
            {
                builder.Metadata(CoverageMarkerKey, marker);
            }
            return builder.Build();
        }

        public const string CoverageMarkerKey = "flowtide.checkpoint";
    }
}
