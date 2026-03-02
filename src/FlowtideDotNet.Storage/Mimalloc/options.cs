using System;
using System.Collections.Generic;
using System.Text;

namespace mimalloctests
{
    public unsafe partial class MiMalloc
    {
        public static partial int mi_option_get(mi_option_t option)
        {
            mi_assert((MI_DEBUG != 0) && (option >= 0) && (option < _mi_option_last));
            ref mi_option_desc_t desc = ref options[(int)option];

            // index should match the option
            mi_assert((MI_DEBUG != 0) && (desc.option == option));

            if (mi_unlikely(desc.init == UNINIT))
            {
                mi_option_init(ref desc);
            }

            return desc.value;
        }
    }
}
