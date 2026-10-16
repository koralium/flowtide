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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    /// <summary>
    /// Columns that turn the source into a change log, every change is sent as an insert.
    /// </summary>
    internal sealed class DeltaHistoryColumns
    {
        public const string Weight = "__weight";
        public const string ModificationDate = "__modificationDate";
        public const string Version = "__version";

        public static readonly IReadOnlyList<string> Names = [Weight, ModificationDate, Version];

        // Output index of each data column
        private readonly int[] _dataIndices;
        private readonly int _columnCount;
        private readonly int _weightIndex;
        private readonly int _modificationDateIndex;
        private readonly int _versionIndex;

        private DeltaHistoryColumns(bool enabled, IReadOnlyList<string> dataColumns, int[] dataIndices, int columnCount, int weightIndex, int modificationDateIndex, int versionIndex)
        {
            Enabled = enabled;
            DataColumns = dataColumns;
            _dataIndices = dataIndices;
            _columnCount = columnCount;
            _weightIndex = weightIndex;
            _modificationDateIndex = modificationDateIndex;
            _versionIndex = versionIndex;
        }

        public bool Enabled { get; }

        /// <summary>
        /// The table columns, in the order the reader returns them.
        /// </summary>
        public IReadOnlyList<string> DataColumns { get; }

        public static bool IsHistoryColumn(string name)
        {
            return Names.Contains(name, StringComparer.OrdinalIgnoreCase);
        }

        public static DeltaHistoryColumns Create(IReadOnlyList<string> names, bool enabled)
        {
            if (!enabled)
            {
                return new DeltaHistoryColumns(false, names, Enumerable.Range(0, names.Count).ToArray(), names.Count, -1, -1, -1);
            }

            var dataColumns = new List<string>();
            var dataIndices = new List<int>();
            int weightIndex = -1;
            int modificationDateIndex = -1;
            int versionIndex = -1;
            for (int i = 0; i < names.Count; i++)
            {
                if (names[i].Equals(Weight, StringComparison.OrdinalIgnoreCase))
                {
                    weightIndex = i;
                }
                else if (names[i].Equals(ModificationDate, StringComparison.OrdinalIgnoreCase))
                {
                    modificationDateIndex = i;
                }
                else if (names[i].Equals(Version, StringComparison.OrdinalIgnoreCase))
                {
                    versionIndex = i;
                }
                else
                {
                    dataColumns.Add(names[i]);
                    dataIndices.Add(i);
                }
            }
            return new DeltaHistoryColumns(true, dataColumns, dataIndices.ToArray(), names.Count, weightIndex, modificationDateIndex, versionIndex);
        }

        public EventBatchWeighted ToBatch(EventBatchData data, PrimitiveList<int> weights, PrimitiveList<uint> iterations, long? commitTime, long version, IMemoryAllocator memoryAllocator)
        {
            if (!Enabled)
            {
                return new EventBatchWeighted(weights, iterations, data);
            }
            return ToBatch(data.Columns.ToArray(), weights, iterations, commitTime, version, memoryAllocator);
        }

        // Data columns at their schema index, deletes become inserts
        public EventBatchWeighted ToBatch(IColumn[] dataColumns, PrimitiveList<int> weights, PrimitiveList<uint> iterations, long? commitTime, long version, IMemoryAllocator memoryAllocator)
        {
            if (!Enabled)
            {
                return new EventBatchWeighted(weights, iterations, new EventBatchData(dataColumns));
            }

            var columns = new IColumn[_columnCount];
            for (int i = 0; i < _dataIndices.Length; i++)
            {
                columns[_dataIndices[i]] = dataColumns[i];
            }

            var count = weights.Count;
            if (_weightIndex >= 0)
            {
                var weightColumn = Column.Create(memoryAllocator);
                for (int i = 0; i < count; i++)
                {
                    weightColumn.Add(new Int64Value(weights[i] < 0 ? -1 : 1));
                }
                columns[_weightIndex] = weightColumn;
            }
            if (_modificationDateIndex >= 0)
            {
                var dateColumn = Column.Create(memoryAllocator);
                if (commitTime.HasValue)
                {
                    var date = new TimestampTzValue(DateTimeOffset.FromUnixTimeMilliseconds(commitTime.Value));
                    for (int i = 0; i < count; i++)
                    {
                        dateColumn.Add(date);
                    }
                }
                else
                {
                    for (int i = 0; i < count; i++)
                    {
                        dateColumn.Add(NullValue.Instance);
                    }
                }
                columns[_modificationDateIndex] = dateColumn;
            }
            if (_versionIndex >= 0)
            {
                var versionColumn = Column.Create(memoryAllocator);
                var versionValue = new Int64Value(version);
                for (int i = 0; i < count; i++)
                {
                    versionColumn.Add(versionValue);
                }
                columns[_versionIndex] = versionColumn;
            }

            for (int i = 0; i < count; i++)
            {
                if (weights[i] < 0)
                {
                    weights.Update(i, -weights[i]);
                }
            }

            return new EventBatchWeighted(weights, iterations, new EventBatchData(columns));
        }

        // First of inCommitTimestamp, timestamp, latest add, latest remove
        public static long? CommitTime(DeltaCommit commit)
        {
            var data = commit.CommitInfo?.Data;
            return ReadTimestamp(data, "inCommitTimestamp")
                ?? commit.CommitInfo?.Timestamp
                ?? LatestAddTime(commit.AddedFiles)
                ?? commit.RemovedFiles.Max(x => x.DeletionTimestamp);
        }

        // Null when no file has a time
        public static long? LatestAddTime(IEnumerable<DeltaAddAction> files)
        {
            return files.Max(x => x.ModificationTime > 0 ? x.ModificationTime : (long?)null);
        }

        // Parsed commits hold JsonElement, our own commits hold long
        private static long? ReadTimestamp(Dictionary<string, object>? data, string key)
        {
            if (data == null || !data.TryGetValue(key, out var value))
            {
                return null;
            }
            return value switch
            {
                long l => l,
                JsonElement e when e.ValueKind == JsonValueKind.Number && e.TryGetInt64(out var v) => v,
                _ => null
            };
        }
    }
}
