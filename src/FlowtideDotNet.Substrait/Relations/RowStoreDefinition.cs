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

namespace FlowtideDotNet.Substrait.Relations
{
    /// <summary>
    /// A relation keeps these output columns in a row store.
    /// Its output is the other columns, then one reference column.
    /// </summary>
    public sealed class RowStoreDefinition : IEquatable<RowStoreDefinition>
    {
        /// <summary>
        /// Name of the store, a fetch reads it by this name.
        /// </summary>
        public required string Name { get; set; }

        /// <summary>
        /// Output positions of the stored columns, before the store is applied.
        /// </summary>
        public required List<int> StoredColumns { get; set; }

        public RowStoreDefinition Clone()
        {
            return new RowStoreDefinition()
            {
                Name = Name,
                StoredColumns = StoredColumns.ToList()
            };
        }

        /// <summary>
        /// The output length with this store.
        /// </summary>
        public int OutputLength(int lengthWithoutStore)
        {
            return lengthWithoutStore - StoredColumns.Count + 1;
        }

        /// <summary>
        /// An output column's position without the store, -1 for the reference.
        /// </summary>
        public int PositionWithoutStore(int outputPosition, int lengthWithoutStore)
        {
            int kept = 0;
            for (int i = 0; i < lengthWithoutStore; i++)
            {
                if (StoredColumns.Contains(i))
                {
                    continue;
                }
                if (kept == outputPosition)
                {
                    return i;
                }
                kept++;
            }
            if (outputPosition == kept)
            {
                return -1;
            }
            throw new ArgumentOutOfRangeException(nameof(outputPosition), $"Row store '{Name}' gives {kept + 1} output columns, got position {outputPosition}.");
        }

        public override bool Equals(object? obj)
        {
            return obj is RowStoreDefinition definition && Equals(definition);
        }

        public bool Equals(RowStoreDefinition? other)
        {
            return other != null &&
                Name == other.Name &&
                StoredColumns.SequenceEqual(other.StoredColumns);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(Name);
            foreach (var column in StoredColumns)
            {
                code.Add(column);
            }
            return code.ToHashCode();
        }
    }
}
