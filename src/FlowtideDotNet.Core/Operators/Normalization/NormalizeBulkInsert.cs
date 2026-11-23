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

using FlowtideDotNet.Core.ColumnStore;

namespace FlowtideDotNet.Core.Operators.Normalization
{
    internal static class NormalizeBulkInsert
    {
        // Inserts rows of the source's columns at the target positions
        public static void InsertFrom(EventBatchData stored, EventBatchData source, List<int> columnsToStore, Span<int> rows, ReadOnlySpan<int> targetPositions)
        {
            int[]? rented = null;
            try
            {
                for (int i = 0; i < columnsToStore.Count; i++)
                {
                    var column = source.Columns[columnsToStore[i]];
                    ReadOnlySpan<int> lookup = rows;
                    if (column is ColumnWithOffset withOffset)
                    {
                        // The stored column reads the inner rows
                        rented ??= System.Buffers.ArrayPool<int>.Shared.Rent(rows.Length);
                        var offsets = withOffset.Offsets;
                        for (int j = 0; j < rows.Length; j++)
                        {
                            rented[j] = offsets[rows[j]];
                        }
                        lookup = rented.AsSpan(0, rows.Length);
                        column = withOffset.InnerColumn;
                    }
                    if (column is AlwaysNullColumn)
                    {
                        // Every row reads as null, no source row is touched
                        rented ??= System.Buffers.ArrayPool<int>.Shared.Rent(rows.Length);
                        rented.AsSpan(0, rows.Length).Fill(ColumnWithOffset.NullValueIndex);
                        lookup = rented.AsSpan(0, rows.Length);
                        stored.Columns[i].InsertFrom(stored.Columns[i], in lookup, in targetPositions, ColumnWithOffset.NullValueIndex);
                        continue;
                    }
                    stored.Columns[i].InsertFrom(column, in lookup, in targetPositions, ColumnWithOffset.NullValueIndex);
                }
            }
            finally
            {
                if (rented != null)
                {
                    System.Buffers.ArrayPool<int>.Shared.Return(rented);
                }
            }
        }
    }
}
