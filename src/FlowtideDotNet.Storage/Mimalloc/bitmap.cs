using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using mi_msecs_t = long;
using mi_theap_malloc = nuint;
using size_t = nuint;
using mi_bfield_t = nuint;
using mi_bchunkmap_t = mimalloctests.MiMalloc.mi_bchunk_t;

namespace mimalloctests
{
    public unsafe partial class MiMalloc
    {
        public const int MI_BFIELD_BITS_SHIFT = (MI_SIZE_SHIFT + 3);
        public const int MI_BFIELD_BITS = (1 << MI_BFIELD_BITS_SHIFT);
        public const int MI_BCHUNK_FIELDS = (MI_BCHUNK_BITS / MI_BFIELD_BITS);
        public const int MI_BCHUNK_SIZE = (MI_BCHUNK_BITS / 8);
        public const int MI_BITMAP_DEFAULT_CHUNK_COUNT = 64;

        [System.Runtime.CompilerServices.InlineArray(MI_BCHUNK_FIELDS)]
        public struct mi_bchunk_t_array
        {
            private Atomic<mi_bfield_t> _element;
        }

        [StructLayout(LayoutKind.Sequential)]
        public unsafe struct mi_bchunk_t
        {
            public mi_bchunk_t_array bfields;
        }


        [System.Runtime.CompilerServices.InlineArray(MI_CBIN_COUNT - 1)]
        public unsafe struct mi_bbitmap_t_chunkmap_bins
        {
            public mi_bchunkmap_t _element;
        }

        [System.Runtime.CompilerServices.InlineArray(MI_BITMAP_DEFAULT_CHUNK_COUNT)]
        public unsafe struct mi_bitmap_t_chunks
        {
            public mi_bchunk_t _element;
        }


        public unsafe struct mi_bitmap_t
        {
            public Atomic<size_t> chunk_count;
            public fixed ulong _padding[MI_BCHUNK_SIZE / MI_SIZE_SIZE - 1];
            public mi_bchunkmap_t chunkmap;
            public mi_bitmap_t_chunks chunks;
        }

        [System.Runtime.CompilerServices.InlineArray(MI_BITMAP_DEFAULT_CHUNK_COUNT)]
        public unsafe struct mi_bbitmap_t_chunks
        {
            public mi_bchunk_t _element;
        }


        [StructLayout(LayoutKind.Sequential)]
        public unsafe struct mi_bbitmap_t
        {
            public Atomic<size_t> chunk_count;
            public Atomic<size_t> chunk_max_accessed;

            private fixed ulong _padding[MI_BCHUNK_SIZE / MI_SIZE_SIZE - 2];

            public mi_bchunkmap_t chunkmap;
            public mi_bbitmap_t_chunkmap_bins chunkmap_bins;
            public mi_bbitmap_t_chunks chunks;

        }
    }
}
