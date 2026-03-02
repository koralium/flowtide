using System;
using System.Collections.Generic;
using System.Text;

namespace mimalloctests
{
    public unsafe partial class MiMalloc
    {
        private static nuint mi_atomic_load_ptr_relaxed(ref nuint p) => Volatile.Read(ref p);
    }
}
