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
    internal class RowHeapOptions
    {
        public int PageTargetBytes { get; set; } = 32 * 1024;

        public int WriteQueueCapacity { get; set; } = 16;

        // A reader holds one page at a time
        public int WindowSize { get; set; } = 1;

        // Test hook, runs on the writer before a page is serialized
        internal Action? PageWriteHookForTests { get; set; }
    }
}
