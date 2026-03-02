using System;
using System.Collections.Generic;
using System.Text;
using static mimalloctests.MiMalloc;

namespace mimalloctests
{
    public unsafe partial class MiMalloc
    {
        private static nuint _mi_theap_random_next(ref mi_theap_t theap)
        {
            return _mi_random_next(ref theap.random);
        }
    }
}
