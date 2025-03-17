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

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    public interface IBatchDecompressor
    {
        /// <summary>
        /// Called once when a new column is being processed.
        /// This allows the batch decompressor to use different dictionaries if needed.
        /// </summary>
        /// <param name="columnIndex"></param>
        void ColumnChange(int columnIndex);

        int Unwrap(ReadOnlySpan<byte> input, Span<byte> output);
    }
}
