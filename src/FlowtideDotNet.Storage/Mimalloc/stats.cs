using System;
using System.Collections.Generic;
using System.Text;

namespace mimalloctests
{
    public unsafe partial class MiMalloc
    {
        public const int MI_CBIN_SMALL = 0;
        public const int MI_CBIN_OTHER = 1;
        public const int MI_CBIN_MEDIUM = 2;
        public const int MI_CBIN_LARGE = 3;
        public const int MI_CBIN_HUGE = 4;
        public const int MI_CBIN_NONE = 5;
        public const int MI_CBIN_COUNT = 6;

        public struct mi_stat_count_t
        {
            public long allocated;

            public long freed;

            public long peak;

            public long current;
        }

        public struct mi_stat_counter_t
        {
            public long total;

            public long count;
        }

        public struct mi_stats_t
        {
            public mi_stat_count_t segments;

            public mi_stat_count_t pages;

            public mi_stat_count_t reserved;

            public mi_stat_count_t committed;

            public mi_stat_count_t reset;

            public mi_stat_count_t page_committed;

            public mi_stat_count_t segments_abandoned;

            public mi_stat_count_t pages_abandoned;

            public mi_stat_count_t threads;

            public mi_stat_count_t huge;

            public mi_stat_count_t giant;

            public mi_stat_count_t malloc;

            public mi_stat_count_t segments_cache;

            public mi_stat_counter_t pages_extended;

            public mi_stat_counter_t mmap_calls;

            public mi_stat_counter_t commit_calls;

            public mi_stat_counter_t page_no_retire;

            public mi_stat_counter_t searches;

            public mi_stat_counter_t huge_count;

            public mi_stat_counter_t giant_count;

            public _normal_e__FixedBuffer normal;

            public partial struct _normal_e__FixedBuffer
            {
                public mi_stat_count_t e0;
                public mi_stat_count_t e1;
                public mi_stat_count_t e2;
                public mi_stat_count_t e3;
                public mi_stat_count_t e4;
                public mi_stat_count_t e5;
                public mi_stat_count_t e6;
                public mi_stat_count_t e7;
                public mi_stat_count_t e8;
                public mi_stat_count_t e9;
                public mi_stat_count_t e10;
                public mi_stat_count_t e11;
                public mi_stat_count_t e12;
                public mi_stat_count_t e13;
                public mi_stat_count_t e14;
                public mi_stat_count_t e15;
                public mi_stat_count_t e16;
                public mi_stat_count_t e17;
                public mi_stat_count_t e18;
                public mi_stat_count_t e19;
                public mi_stat_count_t e20;
                public mi_stat_count_t e21;
                public mi_stat_count_t e22;
                public mi_stat_count_t e23;
                public mi_stat_count_t e24;
                public mi_stat_count_t e25;
                public mi_stat_count_t e26;
                public mi_stat_count_t e27;
                public mi_stat_count_t e28;
                public mi_stat_count_t e29;
                public mi_stat_count_t e30;
                public mi_stat_count_t e31;
                public mi_stat_count_t e32;
                public mi_stat_count_t e33;
                public mi_stat_count_t e34;
                public mi_stat_count_t e35;
                public mi_stat_count_t e36;
                public mi_stat_count_t e37;
                public mi_stat_count_t e38;
                public mi_stat_count_t e39;
                public mi_stat_count_t e40;
                public mi_stat_count_t e41;
                public mi_stat_count_t e42;
                public mi_stat_count_t e43;
                public mi_stat_count_t e44;
                public mi_stat_count_t e45;
                public mi_stat_count_t e46;
                public mi_stat_count_t e47;
                public mi_stat_count_t e48;
                public mi_stat_count_t e49;
                public mi_stat_count_t e50;
                public mi_stat_count_t e51;
                public mi_stat_count_t e52;
                public mi_stat_count_t e53;
                public mi_stat_count_t e54;
                public mi_stat_count_t e55;
                public mi_stat_count_t e56;
                public mi_stat_count_t e57;
                public mi_stat_count_t e58;
                public mi_stat_count_t e59;
                public mi_stat_count_t e60;
                public mi_stat_count_t e61;
                public mi_stat_count_t e62;
                public mi_stat_count_t e63;
                public mi_stat_count_t e64;
                public mi_stat_count_t e65;
                public mi_stat_count_t e66;
                public mi_stat_count_t e67;
                public mi_stat_count_t e68;
                public mi_stat_count_t e69;
                public mi_stat_count_t e70;
                public mi_stat_count_t e71;
                public mi_stat_count_t e72;
                public mi_stat_count_t e73;
            }

        }
    }
}
