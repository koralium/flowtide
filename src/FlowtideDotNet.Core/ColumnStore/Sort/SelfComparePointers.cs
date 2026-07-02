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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Sort
{
    public unsafe struct SelfComparePointers
    {
        /// <summary>
        /// Main data pointer for the self comparison. This will typically point to the current column's data.
        /// </summary>
        public void* dataPointer;

        /// <summary>
        /// Secondary data pointer for the self comparison. This can be used for offsets for binary or string comparisons.
        /// </summary>
        public void* secondaryPointer;

        /// <summary>
        /// Pointer to the validity bitmap if required.
        /// </summary>
        public void* validityPointer;

        /// <summary>
        /// If ColumnWithOffset is used, this pointer contains that information.
        /// </summary>
        public void* columnOffsetsPointer;
    }
}
