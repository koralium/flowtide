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

using System.Runtime.InteropServices;

namespace FlowtideDotNet.Core.ColumnStore.Sort
{
    [StructLayout(LayoutKind.Explicit, Size = 16)]
    public struct RadixItem
    {
        // Evaluated first (Passes 0-3). Least significant sort order.
        [FieldOffset(0)] public uint SecondaryKey;

        // Evaluated last (Passes 4-11). Dominant sort order.
        [FieldOffset(4)] public ulong PrimaryKey;

        // Ignored by Radix passes. Travels safely with the struct.
        [FieldOffset(12)] public int Index;
    }
}
