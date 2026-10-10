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

using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Core.ColumnStore;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    /// <summary>
    /// The reference the catalog's delete candidates are checked against, it asks the statistics comparers.
    /// </summary>
    internal static class CatalogOracle
    {
        // Per file and probe: no statistics, or every pruning column that has a comparer accepts the probe's value
        public static bool MayHold(DeltaStatistics? statistics, PruningLayout layout, IReadOnlyList<IDataValue> probe)
        {
            if (statistics?.ValueComparers == null)
            {
                return true;
            }
            for (int c = 0; c < layout.Columns.Count; c++)
            {
                if (statistics.ValueComparers.TryGetValue(layout.Columns[c].Name, out var comparer) && !comparer.IsInBetween(probe[c]))
                {
                    return false;
                }
            }
            return true;
        }

        // A file's minValues or maxValues as one probe, null when a pruning column has no such bound
        public static IDataValue[]? Bound(string? statistics, StructType schema, PruningLayout layout, string bound)
        {
            if (statistics == null)
            {
                return null;
            }
            using var document = JsonDocument.Parse(statistics);
            if (!document.RootElement.TryGetProperty(bound, out var values))
            {
                return null;
            }
            var probe = new IDataValue[layout.Columns.Count];
            for (int c = 0; c < layout.Columns.Count; c++)
            {
                var column = layout.Columns[c];
                var field = schema.Fields.First(x => string.Equals(x.Name, column.Name, StringComparison.OrdinalIgnoreCase));
                // Matched without case like the statistics parsers match their columns
                var key = field.PhysicalName ?? field.Name;
                var value = values.EnumerateObject().FirstOrDefault(x => string.Equals(x.Name, key, StringComparison.OrdinalIgnoreCase)).Value;
                if (value.ValueKind == JsonValueKind.Undefined || value.ValueKind == JsonValueKind.Null)
                {
                    return null;
                }
                // Other types throw, so the oracle never skips a column silently
                probe[c] = column.Type switch
                {
                    PruningType.Int64 => new Int64Value(value.GetInt64()),
                    PruningType.String => new StringValue(value.GetString()!),
                    _ => throw new NotSupportedException($"The catalog oracle has no conversion for {column.Type}")
                };
            }
            return probe;
        }
    }
}
