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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    /// <summary>
    /// Every field of a live add action, dataChange is set by whoever writes the action.
    /// </summary>
    internal sealed class DeltaFileRecord
    {
        public required string Path { get; init; }

        public Dictionary<string, string>? PartitionValues { get; init; }

        public long Size { get; init; }

        public long ModificationTime { get; init; }

        public string? Statistics { get; init; }

        public Dictionary<string, string>? Tags { get; init; }

        public DeletionVector? DeletionVector { get; init; }

        public long? BaseRowId { get; init; }

        public long? DefaultRowCommitVersion { get; init; }

        public string? ClusteringProvider { get; init; }

        /// <summary>
        /// Parsed once from the statistics, null when they hold none.
        /// </summary>
        public long? NumRecords { get; init; }

        public static DeltaFileRecord FromAdd(DeltaAddAction add, long? numRecords)
        {
            return new DeltaFileRecord()
            {
                Path = add.Path ?? throw new InvalidOperationException("An add action has no path"),
                PartitionValues = add.PartitionValues,
                Size = add.Size,
                ModificationTime = add.ModificationTime,
                Statistics = add.Statistics,
                Tags = add.Tags,
                DeletionVector = add.DeletionVector,
                BaseRowId = add.BaseRowId,
                DefaultRowCommitVersion = add.DefaultRowCommitVersion,
                ClusteringProvider = add.ClusteringProvider,
                NumRecords = numRecords
            };
        }

        public DeltaAddAction ToAdd()
        {
            return new DeltaAddAction()
            {
                Path = Path,
                PartitionValues = PartitionValues,
                Size = Size,
                ModificationTime = ModificationTime,
                DataChange = true,
                Statistics = Statistics,
                Tags = Tags,
                DeletionVector = DeletionVector,
                BaseRowId = BaseRowId,
                DefaultRowCommitVersion = DefaultRowCommitVersion,
                ClusteringProvider = ClusteringProvider
            };
        }

        // Estimate for page sizing, strings counted as UTF-16
        public int ByteSize()
        {
            var size = 64 + Path.Length * 2 + (Statistics?.Length ?? 0) * 2 + (ClusteringProvider?.Length ?? 0) * 2;
            size += MapSize(PartitionValues) + MapSize(Tags);
            if (DeletionVector != null)
            {
                size += 40 + (DeletionVector.StorageType?.Length ?? 0) * 2 + (DeletionVector.PathOrInlineDv?.Length ?? 0) * 2;
            }
            return size;
        }

        private static int MapSize(Dictionary<string, string>? map)
        {
            if (map == null)
            {
                return 0;
            }
            var size = 16;
            foreach (var kv in map)
            {
                size += 16 + kv.Key.Length * 2 + (kv.Value?.Length ?? 0) * 2;
            }
            return size;
        }
    }
}
