using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace mimalloctests
{
    public unsafe partial class MiMalloc
    {
        [ThreadStatic]
        private static mi_theap_t* __mi_theap_default;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_theap_t* _mi_theap_default()
        {
            return __mi_theap_default;
        }

    }
}
