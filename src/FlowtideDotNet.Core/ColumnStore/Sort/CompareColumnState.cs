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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Sort
{
    [Flags]
    public enum CompareColumnState : ushort
    {
        // Lower 8 bits: ArrowTypeId
        TypeMask = 0x00FF,

        // Upper 8 bits: Execution Branches
        HasValidityBitmap = 1 << 8,  // Standard null bitmap (if used)
        IsIndirectView = 1 << 9,  // "Using Offset" -> The data is behind an indirection array
        OffsetContainsNull = 1 << 10 // The -1 fast-null check is required
    }

    public static class CompareColumnStateBuilder
    {
        public const int MaxFastPathColumns = 7;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static CompareColumnState Create(ArrowTypeId arrowTypeId)
        {
            return (CompareColumnState)(byte)arrowTypeId;
        }

        public static void BuildColumnsKey(ref UInt128 key, CompareColumnState state, int index)
        {
            if (index >= MaxFastPathColumns) return;
            key |= ((UInt128)(ushort)state) << (index * 16);
        }

        public static void AddHasTailToKey(ref UInt128 key)
        {
            // Set the 1 bit in the last 16 bits to indicate the presence of a tail column
            key |= (UInt128)1 << 112;
        }
    }
}
