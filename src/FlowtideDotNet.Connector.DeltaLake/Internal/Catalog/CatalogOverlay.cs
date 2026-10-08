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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    /// <summary>
    /// What one staged commit does to the catalog, applied once the commit is published.
    /// </summary>
    internal sealed class CatalogOverlay
    {
        private readonly JsonSerializerOptions _statisticsOptions;

        // Writes and reads statistics for the commit's schema
        public JsonSerializerOptions StatisticsOptions => _statisticsOptions;

        public CatalogOverlay(long version, long adoptedAt, PruningLayout layout, JsonSerializerOptions statisticsOptions, DeltaTable? newHeader)
        {
            Version = version;
            AdoptedAt = adoptedAt;
            Layout = layout;
            _statisticsOptions = statisticsOptions;
            NewHeader = newHeader;
        }

        public long Version { get; }

        public long AdoptedAt { get; }

        // The pruning columns of the rows built for this commit
        public PruningLayout Layout { get; }

        // The table this commit creates
        public DeltaTable? NewHeader { get; }

        // An overwrite removes every file the catalog held
        public bool RemoveAll { get; set; }

        public List<int> Removes { get; } = new List<int>();

        // Files that keep their id, a new deletion vector for the same path
        public List<(int Id, DeltaFileRecord Record)> Updates { get; } = new List<(int, DeltaFileRecord)>();

        public List<(int Id, DeltaFileRecord Record, byte[] Row)> Adds { get; } = new List<(int, DeltaFileRecord, byte[])>();

        /// <summary>
        /// A new file from its add action and the statistics string written into it.
        /// </summary>
        public void Add(int id, Delta.Actions.DeltaAddAction add)
        {
            var statistics = ParseStatistics(add.Statistics, _statisticsOptions);
            var row = new byte[Layout.RowSize];
            Layout.WriteRow(statistics, row);
            Adds.Add((id, DeltaFileRecord.FromAdd(add, statistics?.NumRecords), row));
        }

        /// <summary>
        /// Statistics that cannot be read prune nothing, the file is then a candidate for every delete.
        /// </summary>
        public static DeltaStatistics? ParseStatistics(string? statistics, JsonSerializerOptions options)
        {
            if (statistics == null)
            {
                return null;
            }
            try
            {
                return JsonSerializer.Deserialize<DeltaStatistics>(statistics, options);
            }
            catch (JsonException)
            {
                return null;
            }
            catch (InvalidOperationException)
            {
                return null;
            }
        }
    }
}
