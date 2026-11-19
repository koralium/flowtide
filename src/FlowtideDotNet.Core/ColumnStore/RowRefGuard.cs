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

using FlowtideDotNet.Base.Vertices;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal static class RowRefGuard
    {
        public static NotSupportedException NotAValue(string operation)
        {
            return new NotSupportedException($"A row reference cannot be {operation}, its columns must be fetched first.");
        }

        /// <summary>
        /// Throws when a batch still carries row references, also inside other columns.
        /// </summary>
        public static void ThrowIfPresent(EventBatchData batch, IStreamVertex receiver)
        {
            var columns = batch.Columns;
            for (int i = 0; i < columns.Count; i++)
            {
                if (HoldsRowReferences(columns[i]))
                {
                    // The name is only read for the error
                    throw new InvalidOperationException($"{receiver.DisplayName} received row references in column {i}, their columns must be fetched first.");
                }
            }
        }

        private static bool HoldsRowReferences(IColumn column)
        {
            switch (column.Type)
            {
                case ArrowTypeId.RowRef:
                    return true;
                case ArrowTypeId.Union:
                case ArrowTypeId.Struct:
                case ArrowTypeId.List:
                case ArrowTypeId.Map:
                    return column.DataColumn.HoldsRowReferences;
                default:
                    return false;
            }
        }
    }
}
