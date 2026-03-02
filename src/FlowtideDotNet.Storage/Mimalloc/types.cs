using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using static mimalloctests.MiMalloc;
using mi_msecs_t = long;
using mi_threadid_t = nuint;
using size_t = nuint;
using mi_thread_local_t = nuint;
using mi_lock_t = System.Threading.SpinLock;
using mi_encoded_t = nuint;
using mi_thread_free_t = nuint;

namespace mimalloctests
{
    public unsafe partial class MiMalloc
    {
        // ------------------------------------------------------
        // Konstanter & Storlekar
        // ------------------------------------------------------

        public const int MI_MAX_ALIGN_SIZE = 16; // Ofta 16 för SSE

        // Storlekar för 64-bitars system (MI_SIZE_SHIFT = 3)
        public const int MI_SIZE_SHIFT = 3;
        public const int MI_SIZE_SIZE = 1 << MI_SIZE_SHIFT;
        public const int MI_INTPTR_SIZE = 8;
        public const int MI_INTPTR_SHIFT = 3;

        public const int MI_ARENA_SLICE_SHIFT = 13 + MI_SIZE_SHIFT; // 16 -> 64 KiB
        public const int MI_BCHUNK_BITS_SHIFT = 6 + MI_SIZE_SHIFT;  // 9 -> 512 bits
        public const int MI_BCHUNK_BITS = 1 << MI_BCHUNK_BITS_SHIFT;

        public const nuint MI_ARENA_SLICE_SIZE = 1u << MI_ARENA_SLICE_SHIFT;
        public const nuint MI_ARENA_SLICE_ALIGN = MI_ARENA_SLICE_SIZE;

        public const int MI_ARENA_MIN_OBJ_SLICES = 1;
        public const int MI_ARENA_MAX_CHUNK_OBJ_SLICES = MI_BCHUNK_BITS;

        public const nuint MI_ARENA_MIN_OBJ_SIZE = MI_ARENA_MIN_OBJ_SLICES * MI_ARENA_SLICE_SIZE;
        public const nuint MI_ARENA_MAX_CHUNK_OBJ_SIZE = MI_ARENA_MAX_CHUNK_OBJ_SLICES * MI_ARENA_SLICE_SIZE;

        public const nuint MI_SMALL_PAGE_SIZE = MI_ARENA_MIN_OBJ_SIZE;       // 64 KiB
        public const nuint MI_MEDIUM_PAGE_SIZE = 8 * MI_SMALL_PAGE_SIZE;     // 512 KiB
        public const nuint MI_LARGE_PAGE_SIZE = MI_SIZE_SIZE * MI_MEDIUM_PAGE_SIZE; // 4 MiB

        public const uint MI_BIN_HUGE = 73U;
        public const uint MI_BIN_FULL = MI_BIN_HUGE + 1;
        public const uint MI_BIN_COUNT = MI_BIN_FULL + 1;

        public const nuint MI_PAGE_MIN_COMMIT_SIZE = MI_ARENA_SLICE_SIZE;

        public const nuint MI_KiB = 1024;
        public const nuint MI_PAGE_ALIGN = MI_ARENA_SLICE_ALIGN;
        public const nuint MI_PAGE_MIN_START_BLOCK_ALIGN = MI_MAX_ALIGN_SIZE;
        public const nuint MI_PAGE_MAX_START_BLOCK_ALIGN2 = 4 * MI_KiB;
        public const nuint MI_PAGE_OSPAGE_BLOCK_ALIGN2 = 4 * MI_KiB;
        public const nuint MI_PAGE_MAX_OVERALLOC_ALIGN = MI_ARENA_SLICE_SIZE;

        public const nuint MI_SMALL_MAX_OBJ_SIZE = (MI_SMALL_PAGE_SIZE - MI_PAGE_OSPAGE_BLOCK_ALIGN2) / 6;

#if MI_ENABLE_LARGE_PAGES
        public const nuint MI_MEDIUM_MAX_OBJ_SIZE = (MI_MEDIUM_PAGE_SIZE - MI_PAGE_OSPAGE_BLOCK_ALIGN2) / 6;
        public const nuint MI_LARGE_MAX_OBJ_SIZE = MI_LARGE_PAGE_SIZE / 8;
#else
        public const nuint MI_MEDIUM_MAX_OBJ_SIZE = MI_MEDIUM_PAGE_SIZE / 8;
        public const nuint MI_LARGE_MAX_OBJ_SIZE = MI_MEDIUM_MAX_OBJ_SIZE;
#endif

        // Page Flags
        public const nuint MI_PAGE_IN_FULL_QUEUE = 0x01;
        public const nuint MI_PAGE_HAS_INTERIOR_POINTERS = 0x02;
        public const nuint MI_PAGE_FLAG_MASK = 0x03;

        public const nuint MI_THREADID_ABANDONED = 0;
        public const nuint MI_THREADID_ABANDONED_MAPPED = MI_PAGE_FLAG_MASK + 1;

        public const int MI_PAGES_DIRECT = 130; // Förenklad konstant (beräknas i C via MI_SMALL_WSIZE_MAX etc)

        public const int MI_ARENA_BIN_COUNT = (int)MI_BIN_COUNT;

        public const int MI_MAX_ARENAS = 160;

        // ------------------------------------------------------
        // Enums
        // ------------------------------------------------------

        public unsafe struct Atomic<T>
        {
            public T value;
        }

        [StructLayout(LayoutKind.Sequential)]
        public unsafe struct MiMemidOsInfo
        {
            public void* @base;
            public size_t size;
        }

        [StructLayout(LayoutKind.Sequential)]
        public unsafe struct MiMemidArenaInfo
        {
            public mi_arena_t* arena;
            public uint slice_index;
            public uint slice_count;
        }

        [StructLayout(LayoutKind.Sequential)]
        public unsafe struct MiMemidMetaInfo
        {
            public void* meta_page;
            public uint block_index;
            public uint block_count;
        }

        public enum mi_memkind_t : byte
        {
            MI_MEM_NONE,
            MI_MEM_EXTERNAL,
            MI_MEM_STATIC,
            MI_MEM_META,
            MI_MEM_OS,
            MI_MEM_OS_HUGE,
            MI_MEM_OS_REMAP,
            MI_MEM_ARENA,
            MI_MEM_HEAP_MAIN
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static bool mi_memkind_is_os(mi_memkind_t memkind)
        {
            return (memkind >= mi_memkind_t.MI_MEM_OS && memkind <= mi_memkind_t.MI_MEM_OS_REMAP);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static bool mi_memkind_needs_no_free(mi_memkind_t memkind)
        {
            return (memkind <= mi_memkind_t.MI_MEM_STATIC);
        }

        public enum MiPageKind : byte
        {
            MI_PAGE_SMALL,
            MI_PAGE_MEDIUM,
            MI_PAGE_LARGE,
            MI_PAGE_SINGLETON
        }

        [StructLayout(LayoutKind.Explicit)]
        public struct MiMemidUnion
        {
            [FieldOffset(0)] public MiMemidOsInfo os;
            [FieldOffset(0)] public MiMemidArenaInfo arena;
            [FieldOffset(0)] public MiMemidMetaInfo meta;
        }

        [StructLayout(LayoutKind.Sequential)]
        public unsafe struct mi_memid_t
        {
            public MiMemidUnion mem;
            public mi_memkind_t memkind;
            public bool is_pinned;
            public bool initially_committed;
            public bool initially_zero;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_memid_is_os(mi_memid_t memid)
        {
            return mi_memkind_is_os(memid.memkind);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_memid_needs_no_free(mi_memid_t memid)
        {
            return mi_memkind_needs_no_free(memid.memkind);
        }

        public static mi_arena_t* mi_memid_arena(mi_memid_t memid)
        {
            return (memid.memkind == mi_memkind_t.MI_MEM_ARENA ? memid.mem.arena.arena : null);
        }

        [System.Runtime.CompilerServices.InlineArray(MI_ARENA_BIN_COUNT)]
        public unsafe struct mi_arena_pages_t_pages_abandoned
        {
            public nuint _element;

            public mi_bitmap_t* Get(int index)
            {
                return (mi_bitmap_t*)this[index];
            }

            public void Set(int index, mi_bitmap_t* value)
            {
                this[index] = (nuint)value;
            }
        }


        public unsafe struct mi_arena_pages_t
        {
            public mi_bitmap_t* pages;
            public mi_arena_pages_t_pages_abandoned pages_abandoned;
        }

        public unsafe struct mi_arena_t
        {
            public mi_memid_t memid;
            public mi_subproc_t* subproc;
            public size_t arena_idx;
            public size_t slice_count;
            public size_t info_slices;
            public int numa_node;
            public bool is_exclusive;
            public Atomic<mi_msecs_t> purge_expire;
            public delegate* unmanaged<void> commit_fun;
            public void* commit_fun_arg;
            public mi_bbitmap_t* slices_free;
            public mi_bitmap_t* slices_committed;
            public mi_bitmap_t* slices_dirty;
            public mi_bitmap_t* slices_purge;
            public mi_arena_pages_t pages_main;
        }

        [InlineArray(MI_MAX_ARENAS)]
        public unsafe struct mi_subproc_t_arenas
        {
            public AtomicPtr<mi_arena_t> elements;
        }

        public unsafe struct mi_subproc_t
        {
            public size_t subproc_seq;
            public mi_subproc_t* next;
            public mi_subproc_t* prev;
            public Atomic<size_t> arena_count;
            public mi_subproc_t_arenas arenas;
            public mi_lock_t arena_reserve_lock;
            public Atomic<long> purge_expire;
            public AtomicPtr<mi_heap_t> heap_main;
            public mi_heap_t* heaps;
            public mi_lock_t heaps_lock;
            public Atomic<size_t> thread_count;
            public Atomic<size_t> thread_total_count;
            public Atomic<size_t> heap_count;
            public Atomic<size_t> heap_total_count;
            public mi_memid_t memid;
            public mi_stats_t stats;

            // DONE
        }

        public struct mi_tld_t
        {
            public mi_threadid_t thread_id;
            public size_t thread_seq;
            public int numa_node;
            public mi_subproc_t* subproc;
            public mi_theap_t* theaps;
            public bool recurse;
            public bool is_in_threadpool;
            public mi_memid_t memid;
            // DONE
        }

        public unsafe struct AtomicPtr<T> where T : unmanaged
        {
            public nuint value;

            public T* Ptr
            {
                get => (T*)value;
                set => this.value = (nuint)value;
            }
        }

        public unsafe struct mi_block_t
        {
            public mi_encoded_t next;
        }

        [InlineArray(2)]
        public unsafe struct mi_page_t_padding
        {
            public nuint _element;
        }

        public unsafe struct mi_page_t
        {
            public Atomic<mi_threadid_t> xthread_id;
            public mi_block_t* free;
            public ushort used;
            public ushort capacity;
            public ushort reserved;
            public byte retire_expire;
            public bool free_is_zero;
            public Atomic<mi_thread_free_t> xthread_free;
            public size_t block_size;
            public byte* page_start;
            public mi_page_t_padding keys;
            public mi_theap_t* theap;
            public mi_heap_t* heap;
            public mi_page_t* next;
            public mi_page_t* prev;
            public size_t slice_committed;
            public mi_memid_t memid;
            // DONE
        }

        [System.Runtime.CompilerServices.InlineArray((int)MI_BIN_COUNT)]
        public unsafe struct mi_heap_t_abandoned_count
        {
            public Atomic<size_t> _element;
        }

        [InlineArray(MI_MAX_ARENAS)]
        public unsafe struct mi_heap_t_arena_pages
        {
            public AtomicPtr<mi_arena_pages_t> _element;
        }

        [StructLayout(LayoutKind.Sequential)]
        public unsafe struct mi_heap_t
        {
            public mi_subproc_t* subproc;
            public size_t heap_seq;
            public mi_heap_t* next;
            public mi_heap_t* prev;
            public mi_thread_local_t theap;
            public mi_arena_t* exclusive_arena;
            public int numa_node;
            public mi_theap_t* theaps;
            public mi_lock_t theaps_lock;
            public mi_heap_t_abandoned_count abandoned_count;
            public mi_page_t* os_abandoned_pages;
            public mi_lock_t os_abandoned_pages_lock;
            public mi_heap_t_arena_pages arena_pages;
            public mi_lock_t arena_pages_lock;
            public mi_stats_t stats;
            // DONE
        }

        public unsafe struct mi_random_ctx_t
        {
            public fixed uint input[16];
            public fixed uint output[16];
            public int output_available;
            public bool weak;
        }

        public unsafe struct mi_page_queue_t
        {
            public mi_page_t* first;
            public mi_page_t* last;
            public size_t count;
            public size_t block_size;
        }

        [InlineArray(MI_PAGES_DIRECT)]
        public unsafe struct mi_theap_t_pages_free_direct
        {
            private nuint _element;

            public mi_page_t* Get(int index)
            {
                return (mi_page_t*)this[index];
            }

            public void Set(int index, mi_page_t* value)
            {
                this[index] = (nuint)value;
            }
        }

        [InlineArray((int)MI_BIN_COUNT)]
        public unsafe struct mi_theap_t_pages
        {
            private mi_page_queue_t _element;
        }

        // A thread-local heap ("theap") owns a set of thread-local pages.
        public unsafe struct mi_theap_t
        {
            public mi_tld_t* tld;
            public mi_heap_t* heap;
            public ulong heartbeat;
            public nuint cookie;
            public mi_random_ctx_t random;
            public size_t page_count;
            public size_t page_retired_min;
            public size_t page_retired_max;
            public long generic_count;
            public long generic_collect_count;
            public mi_theap_t* tnext;                               // list of theaps in this thread
            public mi_theap_t* tprev;
            public  mi_theap_t* hnext;                               // list of theaps of the owning `heap`
            public  mi_theap_t* hprev;

            public long page_full_retain;                    // how many full pages can be retained per queue (before abandoning them)
            public bool allow_page_reclaim;                  // `true` if this theap should not reclaim abandoned pages
            public bool allow_page_abandon;                  // `true` if this theap can abandon pages to reduce memory footprint

            // Skip guarded

            public mi_theap_t_pages_free_direct pages_free_direct;  // optimize: array where every entry points a page with possibly free blocks in the corresponding queue for that size.
            public mi_theap_t_pages pages;                          // queue of pages for each size class (or "bin")
            public mi_memid_t memid;                               // provenance of the theap struct itself (meta or os)
            public mi_stats_t stats;

            // NOT DONE
        }
    }
}
