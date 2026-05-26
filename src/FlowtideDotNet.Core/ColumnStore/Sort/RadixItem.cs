using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

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
