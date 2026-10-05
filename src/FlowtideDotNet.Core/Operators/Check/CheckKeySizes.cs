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

using FlowtideDotNet.Core.ColumnStore.TreeStorage;

namespace FlowtideDotNet.Core.Operators.Check
{
    internal static class CheckKeySizes
    {
        /// <summary>
        /// Byte size prefix sum of issue keys with an int value.
        /// </summary>
        public static void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
        {
            if (indices.Length == 0)
            {
                return;
            }
            // Sparse keys, only applied rows are set
            var columns = keys[indices[0]].referenceBatch.GetColumns_Unsafe();
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i].GetPrefixSumByteSizes(indices, sizes);
            }

            var cumulativeValueBytes = 0;
            for (int i = 0; i < indices.Length; i++)
            {
                cumulativeValueBytes += sizeof(int);
                sizes[i] += cumulativeValueBytes;
            }
        }
    }
}
