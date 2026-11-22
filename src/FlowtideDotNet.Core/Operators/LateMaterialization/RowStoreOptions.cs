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

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    internal enum RowStoreMode
    {
        // Rows are only added
        Append = 1,
        // The host knows rows by key and retires old versions
        Keyed = 2
    }

    internal class RowStoreOptions
    {
        public RowStoreMode Mode { get; set; } = RowStoreMode.Append;

        public RowHeapOptions Heap { get; set; } = new RowHeapOptions();

        // Retired rows kept in memory before a temporary tree
        public int RetireBufferSize { get; set; } = 8192;
    }
}
