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
//
// Port of mimalloc v3.4.4 `include/mimalloc/types.h` (+ size constants from `mimalloc.h`).
// Original: Copyright (c) 2018-2026 Microsoft Research, Daan Leijen (MIT license).
//
// Pinned configuration (see README.md): 64-bit, MI_SECURE=0, MI_PADDING=0,
// MI_ENCODE_FREELIST=0, MI_GUARDED=0, MI_ENABLE_LARGE_PAGES=1, MI_PAGE_MAP_FLAT=0,
// MI_PAGE_META_IS_SEPARATED=1. Fields guarded by disabled defines (e.g. `keys[2]`)
// are omitted from the structs.
//
// Type mapping: size_t -> nuint, uintptr_t -> nuint, ptrdiff_t/intptr_t -> nint,
// int64_t -> long, C `long` -> C# long (64-bit on all our platforms; the C code has
// 32-bit `long` on Windows but only uses it for counters/options where width is not
// semantic). `_Atomic(T)` fields are plain fields accessed via the Mi.mi_atomic_*
// helpers only. All structs live in native memory; the GC never traces them.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        // Minimal alignment necessary. On most platforms 16 bytes are needed
        // due to SSE registers for example. This must be at least `sizeof(void*)`
        public const int MI_MAX_ALIGN_SIZE = 16;

        // --------------------------------------------------------------
        // Sizes of internal data-structures
        // (all values for the pinned 64-bit configuration)
        // --------------------------------------------------------------

        // Main size parameter; determines max arena sizes and max arena object sizes etc.
        public const int MI_ARENA_SLICE_SHIFT = 13 + MI_SIZE_SHIFT;               // 16: 64 KiB
        public const int MI_BCHUNK_BITS_SHIFT = 6 + MI_SIZE_SHIFT;                // 9: 512 bits

        public const int MI_BCHUNK_BITS = 1 << MI_BCHUNK_BITS_SHIFT;              // 512; sub-bitmaps in arena's are "bchunks" of 512 bits
        public const nuint MI_ARENA_SLICE_SIZE = (nuint)1 << MI_ARENA_SLICE_SHIFT; // 64 KiB; arena's allocate in slices
        public const nuint MI_ARENA_SLICE_ALIGN = MI_ARENA_SLICE_SIZE;

        public const int MI_ARENA_MIN_OBJ_SLICES = 1;
        public const int MI_ARENA_MAX_CHUNK_OBJ_SLICES = MI_BCHUNK_BITS;          // 32 MiB

        public const nuint MI_ARENA_MIN_OBJ_SIZE = MI_ARENA_MIN_OBJ_SLICES * MI_ARENA_SLICE_SIZE;
        public const ulong MI_ARENA_MAX_CHUNK_OBJ_SIZE = (ulong)MI_ARENA_MAX_CHUNK_OBJ_SLICES * MI_ARENA_SLICE_SIZE;

        public const nuint MI_SMALL_PAGE_SIZE = MI_ARENA_MIN_OBJ_SIZE;            // 64 KiB
        public const nuint MI_MEDIUM_PAGE_SIZE = 8 * MI_SMALL_PAGE_SIZE;          // 512 KiB (=byte in the bchunk bitmap)
        public const nuint MI_LARGE_PAGE_SIZE = MI_SIZE_SIZE * MI_MEDIUM_PAGE_SIZE; // 4 MiB (=word in the bchunk bitmap)

        // Maximum number of size classes. (spaced exponentially in 12.5% increments)
        public const int MI_BIN_HUGE = 73;
        public const int MI_BIN_FULL = MI_BIN_HUGE + 1;
        public const int MI_BIN_COUNT = MI_BIN_FULL + 1;

        // We never allocate more than PTRDIFF_MAX
        public const ulong MI_MAX_ALLOC_SIZE = long.MaxValue;

        // Minimal commit for a page on-demand commit (should be >= OS page size)
        public const nuint MI_PAGE_MIN_COMMIT_SIZE = MI_ARENA_SLICE_SIZE;

        // from mimalloc.h
        public const int MI_SMALL_WSIZE_MAX = 128;
        public const nuint MI_SMALL_SIZE_MAX = MI_SMALL_WSIZE_MAX * (nuint)MI_INTPTR_SIZE;   // 1 KiB

        // MI_PADDING == 0
        public const int MI_PADDING_SIZE = 0;
        public const int MI_PADDING_WSIZE = 0;

        public const int MI_PAGES_DIRECT = MI_SMALL_WSIZE_MAX + MI_PADDING_WSIZE + 1;  // 129

        // ------------------------------------------------------
        // Page flags (in the bottom 2 bits of `mi_page_t.xthread_id`)
        // ------------------------------------------------------

        public const nuint MI_PAGE_IN_FULL_QUEUE = 0x01;
        public const nuint MI_PAGE_HAS_INTERIOR_POINTERS = 0x02;
        public const nuint MI_PAGE_FLAG_MASK = 0x03;

        // Special threadid's: 0 for pages that are abandoned (and not in a theap queue),
        // and 4 for abandoned pages that are also mapped in an arena's `pages_abandoned` bitmap.
        public const nuint MI_THREADID_ABANDONED = 0;
        public const nuint MI_THREADID_ABANDONED_MAPPED = MI_PAGE_FLAG_MASK + 1;   // 4

        // ------------------------------------------------------
        // Object sizes
        // ------------------------------------------------------

        public const nuint MI_PAGE_ALIGN = MI_ARENA_SLICE_ALIGN;          // pages must be aligned on this for the page map
        public const nuint MI_PAGE_MIN_START_BLOCK_ALIGN = MI_MAX_ALIGN_SIZE;  // minimal block alignment for the first block in a page (16b)
        public const nuint MI_PAGE_MAX_START_BLOCK_ALIGN2 = 4 * MI_KiB;   // maximal block alignment for "power of 2"-sized blocks
        public const nuint MI_PAGE_OSPAGE_BLOCK_ALIGN2 = 4 * MI_KiB;      // also aligns any multiple of this size to avoid TLB misses
        public const nuint MI_PAGE_MAX_OVERALLOC_ALIGN = MI_ARENA_SLICE_SIZE;  // (64 KiB) limit for which we overallocate in arena pages

        // The max object sizes are intended to not waste more than ~12.5% internally over the page sizes.
        // MI_ENABLE_LARGE_PAGES == 1
        public const nuint MI_SMALL_MAX_OBJ_SIZE = (MI_SMALL_PAGE_SIZE - MI_PAGE_OSPAGE_BLOCK_ALIGN2) / 6;   // = 10 KiB
        public const nuint MI_MEDIUM_MAX_OBJ_SIZE = (MI_MEDIUM_PAGE_SIZE - MI_PAGE_OSPAGE_BLOCK_ALIGN2) / 6; // ~ 84 KiB
        public const nuint MI_LARGE_MAX_OBJ_SIZE = MI_LARGE_PAGE_SIZE / 8;   // 512 KiB; must be a nice power of 2 for `_mi_bin`
        public const nuint MI_LARGE_MAX_OBJ_WSIZE = MI_LARGE_MAX_OBJ_SIZE / MI_SIZE_SIZE;   // 64 Ki words

        // static invariant: MI_MAX_SINGLETON_BIN >= _mi_bin(MI_LARGE_MAX_OBJ_SIZE)
        // (MI_LARGE_MAX_OBJ_WSIZE == 65536 -> 60)
        public const int MI_MAX_SINGLETON_BIN = 60;

        // ------------------------------------------------------
        // Arena's
        // ------------------------------------------------------

        public const int MI_ARENA_BIN_COUNT = MI_MAX_SINGLETON_BIN + 1;   // 61
        public const ulong MI_ARENA_MIN_SIZE = (ulong)MI_BCHUNK_BITS * MI_ARENA_SLICE_SIZE;        // 32 MiB
        // MI_BITMAP_MAX_BIT_COUNT == MI_BCHUNK_BITS * MI_BCHUNK_BITS (verified against bitmap.h when porting)
        public const ulong MI_ARENA_MAX_SIZE = (ulong)MI_BCHUNK_BITS * MI_BCHUNK_BITS * MI_ARENA_SLICE_SIZE;  // 16 GiB

        public const int MI_MAX_ARENAS = 160;   // 160 arenas is enough for ~2 TiB memory (arenas scale up exponentially)

        // ------------------------------------------------------
        // Error codes passed to `_mi_fatal_error`
        // ------------------------------------------------------

        public const int EAGAIN = 11;      // double free
        public const int ENOMEM = 12;      // out of memory
        public const int EFAULT = 14;      // corrupted free-list or meta-data
        public const int EINVAL = 22;      // trying to free an invalid pointer
        public const int EOVERFLOW = 75;   // count*size overflow
        public const int ENOENT = 2;       // environment variable not found
        public const int ETIMEDOUT = 110;  // timed out (used by huge page reservation)

        // ------------------------------------------------------
        // Debug constants
        // ------------------------------------------------------

        public const byte MI_DEBUG_UNINIT = 0xD0;
        public const byte MI_DEBUG_FREED = 0xDF;
        public const byte MI_DEBUG_PADDING = 0xDE;

        // ------------------------------------------------------
        // mi_memid_t helpers (static inline in types.h)
        // ------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_memkind_is_os(mi_memkind_t memkind)
            => memkind >= mi_memkind_t.MI_MEM_OS && memkind <= mi_memkind_t.MI_MEM_OS_REMAP;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_memkind_needs_no_free(mi_memkind_t memkind)
            => memkind <= mi_memkind_t.MI_MEM_STATIC;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_memid_is_os(in mi_memid_t memid) => mi_memkind_is_os(memid.memkind);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_memid_needs_no_free(in mi_memid_t memid) => mi_memkind_needs_no_free(memid.memkind);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static mi_arena_t* mi_memid_arena(in mi_memid_t memid)
            => memid.memkind == mi_memkind_t.MI_MEM_ARENA ? memid.mem.arena.arena : null;
    }

    // ---------------------------------------------------------------
    // a memory id tracks the provenance of arena/OS allocated memory
    // ---------------------------------------------------------------

    internal enum mi_memkind_t : int
    {
        MI_MEM_NONE,      // not allocated
        MI_MEM_EXTERNAL,  // not owned by mimalloc but provided externally
        MI_MEM_STATIC,    // allocated in a static area and should not be freed
        MI_MEM_META,      // allocated with the meta data allocator (`ArenaMeta.cs`)
        MI_MEM_OS,        // allocated from the OS
        MI_MEM_OS_HUGE,   // allocated as huge OS pages (usually 1GiB, pinned to physical memory)
        MI_MEM_OS_REMAP,  // allocated in a remapable area (i.e. using `mremap`)
        MI_MEM_ARENA,     // allocated from an arena (the usual case) (`Arena.cs`)
        MI_MEM_HEAP_MAIN  // allocated in the main heap (for theaps)
    }

    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct mi_memid_os_info_t
    {
        public void* @base;               // actual base address of the block (used for offset aligned allocations)
        public nuint size;                // allocated full size
    }

    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct mi_memid_arena_info_t
    {
        public mi_arena_t* arena;         // arena that contains this memory
        public uint slice_index;          // slice index in the arena
        public uint slice_count;          // allocated slices
    }

    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct mi_memid_meta_info_t
    {
        public mi_meta_page_t* meta_page; // meta-page that contains the block
        public uint block_index;          // block index in the meta-data page
        public uint block_count;          // allocated blocks
    }

    [StructLayout(LayoutKind.Explicit)]
    internal unsafe struct mi_memid_mem_t   // union
    {
        [FieldOffset(0)] public mi_memid_os_info_t os;       // only used for MI_MEM_OS
        [FieldOffset(0)] public mi_memid_arena_info_t arena; // only used for MI_MEM_ARENA
        [FieldOffset(0)] public mi_memid_meta_info_t meta;   // only used for MI_MEM_META
    }

    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct mi_memid_t
    {
        public mi_memid_mem_t mem;
        public mi_memkind_t memkind;
        public bool is_pinned;            // `true` if we cannot decommit/reset/protect in this memory
        public bool initially_committed;  // `true` if the memory was originally allocated as committed
        public bool initially_zero;       // `true` if the memory was originally zero initialized
    }

    // ------------------------------------------------------
    // Mimalloc pages contain allocated blocks
    // ------------------------------------------------------

    // free lists contain blocks (mi_encoded_t next; MI_ENCODE_FREELIST == 0 so plain pointer value)
    [StructLayout(LayoutKind.Sequential)]
    internal struct mi_block_t
    {
        public nuint next;
    }

    // A page contains blocks of one specific size (`block_size`).
    // See types.h for the full invariant documentation:
    //  `used - |thread_free|` == actual blocks that are in use (alive)
    //  `used - |thread_free| + |free| + |local_free| == capacity`
    // Non-atomic fields can only be accessed with _ownership_ (low bit of `xthread_free` is 1).
    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct mi_page_t
    {
        public nuint xthread_id;          // atomic; thread this page belongs to. (= `theap->tld->thread_id (or 0 or 4 if abandoned) | page_flags`)

        public mi_block_t* free;          // list of available free blocks (`malloc` allocates from this list)
        public ushort used;               // number of blocks in use (including blocks in `thread_free`)
        public ushort capacity;           // number of blocks committed
        public ushort reserved;           // number of blocks reserved in memory
        public byte retire_expire;        // expiration count for retired blocks
        public bool free_is_zero;         // `true` if the blocks in the free list are zero initialized

        public mi_block_t* local_free;    // list of deferred free blocks by this thread (migrates to `free`)
        public nuint xthread_free;        // atomic; list of deferred free blocks freed by other threads (= `mi_block_t* | (1 if owned)`)

        public nuint block_size;          // const: size available in each block (always `>0`)
        public uint page_ma_offset;       // const: offset relative to the page (in MI_MAX_ALIGN_SIZE parts) to the start of the blocks
        public uint slice_committed;      // committed size relative to the first arena slice of the page data (or 0 if fully committed already)

        // (keys[2] omitted: MI_ENCODE_FREELIST == 0 && MI_PADDING == 0)

        public mi_theap_t* theap;         // the theap owning this page (may not be valid or NULL for abandoned pages)
        public mi_heap_t* heap;           // const: the heap owning this page

        public mi_page_t* next;           // next page owned by the theap with the same `block_size`
        public mi_page_t* prev;           // previous page owned by the theap with the same `block_size`
        public mi_memid_t memid;          // const: provenance of the page memory
    }

    // ------------------------------------------------------
    // Page kinds
    // ------------------------------------------------------

    internal enum mi_page_kind_t : int
    {
        MI_PAGE_SMALL,      // small blocks go into 64KiB pages
        MI_PAGE_MEDIUM,     // medium blocks go into 512KiB pages
        MI_PAGE_LARGE,      // larger blocks go into 4MiB pages (MI_ENABLE_LARGE_PAGES == 1)
        MI_PAGE_SINGLETON   // page containing a single block
    }

    // ------------------------------------------------------
    // Thread local heaps ("theap")
    // ------------------------------------------------------

    // Pages of a certain block size are held in a queue.
    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct mi_page_queue_t
    {
        public mi_page_t* first;
        public mi_page_t* last;
        public nuint count;
        public nuint block_size;
    }

    // Random context
    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct mi_random_ctx_t
    {
        public fixed uint input[16];
        public fixed uint output[16];
        public int output_available;
        public bool weak;
    }

    [InlineArray(Mi.MI_BIN_COUNT)]
    internal struct mi_page_queue_array_t
    {
        private mi_page_queue_t _element0;
    }

    // A thread-local heap ("theap") owns a set of thread-local pages.
    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct mi_theap_t
    {
        public mi_tld_t* tld;             // thread-local data
        public mi_heap_t* heap;           // atomic; the heap this theap belongs to
        public mi_subproc_t* subproc;     // atomic; subproc this belongs to (always `subproc == heap->subproc` but needed for safe destruction)
        public nuint refcount;            // atomic; reference count
        public nuint freed;               // atomic; ensure atomic free-ing
        public ulong heartbeat;           // monotonic heartbeat count
        public nuint cookie;              // random cookie to verify pointers (see `_mi_ptr_cookie`)
        public mi_random_ctx_t random;    // random number context used for secure allocation
        public nuint page_count;          // total number of pages in the `pages` queues
        public nuint page_retired_min;    // smallest retired index (retired pages are fully free, but still in the page queues)
        public nuint page_retired_max;    // largest retired index into the `pages` array
        public nuint pages_full_size;     // optimization: total size of blocks in the pages of the full queue (issue #1220)
        public long generic_count;        // how often is `_mi_malloc_generic` called?
        public long generic_collect_count; // how often is `_mi_malloc_generic` called without collecting?

        public mi_theap_t* tnext;         // list of theaps in this thread
        public mi_theap_t* tprev;
        public mi_theap_t* hnext;         // list of theaps of the owning `heap`
        public mi_theap_t* hprev;

        public long page_full_retain;     // how many full pages can be retained per queue (before abandoning them)
        public bool allow_page_reclaim;   // `true` if this theap can reclaim abandoned pages
        public bool allow_page_abandon;   // `true` if this theap can abandon pages to reduce memory footprint

        // (MI_GUARDED fields omitted)

        // optimize: array where every entry points to a page with possibly free blocks
        // in the corresponding queue for that size (elements are mi_page_t*).
        public fixed ulong pages_free_direct[Mi.MI_PAGES_DIRECT];
        public mi_page_queue_array_t pages;  // queue of pages for each size class (or "bin")
        public mi_memid_t memid;          // provenance of the theap struct itself (meta or os)
        public mi_stats_t stats;          // thread-local statistics
    }

    // ------------------------------------------------------
    // Heaps
    // ------------------------------------------------------

    // Keep track of all owned and abandoned pages in the arena's
    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct mi_arena_pages_t
    {
        public mi_bitmap_t* pages;        // all registered pages (abandoned and owned)
        // abandoned pages per size bin (a set bit means the start of the page);
        // elements are mi_bitmap_t*.
        public fixed ulong pages_abandoned[Mi.MI_ARENA_BIN_COUNT];
        // followed by the bitmaps (whose sizes depend on the arena size)
    }

    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct mi_heap_t
    {
        public mi_subproc_t* subproc;     // a heap belongs to a subprocess
        public nuint heap_seq;            // unique sequence number for heaps in this subprocess
        public mi_heap_t* next;           // list of heaps in this subprocess
        public mi_heap_t* prev;
        public nuint theap;               // mi_thread_local_t: dynamic thread local for the thread-local theaps of this heap

        public mi_arena_t* exclusive_arena;  // if the heap should only allocate from a specific arena (or NULL)
        public int numa_node;             // if >=0, prefer this numa node for allocations

        public mi_theap_t* theaps;        // list of all thread-local theaps belonging to this heap (using `hnext`/`hprev`)
        public mi_lock_t theaps_lock;     // lock for the theaps list operations

        public fixed ulong abandoned_count[Mi.MI_BIN_COUNT];  // atomic size_t[]; total count of abandoned pages in this heap
        public mi_page_t* os_abandoned_pages;      // list of pages that are OS allocated and not in an arena
        public mi_lock_t os_abandoned_pages_lock;  // lock for the os abandoned pages list

        // atomic mi_arena_pages_t*[]; track owned and abandoned pages in the arenas (entries can be NULL)
        public fixed ulong arena_pages[Mi.MI_MAX_ARENAS];
        public mi_lock_t arena_pages_lock;         // lock to update the arena_pages array

        public mi_stats_t stats;          // statistics for this heap; periodically updated by merging from each theap
    }

    // ------------------------------------------------------
    // Sub processes
    // ------------------------------------------------------

    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct mi_subproc_t
    {
        public nuint subproc_seq;         // unique id for sub-processes
        public mi_subproc_t* next;        // list of all sub-processes
        public mi_subproc_t* prev;
        public mi_meta_page_t* meta_pages;  // atomic; meta data pages

        public nuint arena_count;         // atomic; current count of arena's
        public fixed ulong arenas[Mi.MI_MAX_ARENAS];   // atomic mi_arena_t*[]; arena's of this sub-process
        public mi_lock_t arena_reserve_lock;           // lock to ensure arena's get reserved one at a time
        public long purge_expire;         // atomic; expiration is set if any arenas can be purged

        public mi_heap_t* heap_main;      // atomic; main heap for this sub process
        public mi_heap_t* heaps;          // heaps belonging to this sub-process
        public mi_lock_t heaps_lock;

        public nuint thread_count;        // atomic; current threads associated with this sub-process
        public nuint thread_total_count;  // atomic; total created threads associated with this sub-process
        public nuint heap_count;          // atomic; current heaps in this sub-process (== |heaps|)
        public nuint heap_total_count;    // atomic; total created heaps in this sub-process

        public mi_memid_t memid;          // provenance of this memory block (meta or static)
        public mi_subproc_t* parent;      // subproc in which this one was allocated
        public mi_stats_t stats;          // subprocess statistics
    }

    // ------------------------------------------------------
    // Thread Local data
    // ------------------------------------------------------

    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct mi_tld_t
    {
        public nuint thread_id;           // thread id of this thread
        public nuint thread_seq;          // thread sequence id (linear count of created threads)
        public int numa_node;             // thread preferred numa node
        public mi_subproc_t* subproc;     // sub-process this thread belongs to
        public mi_theap_t* theaps;        // list of theaps in this thread (so we can abandon all when the thread terminates)
        public mi_lock_t theaps_lock;     // lock as the theaps list is sometimes accessed from another thread (on `mi_heap_free`)
        public bool recurse;              // true if deferred was called; used to prevent infinite recursion
        public bool is_in_threadpool;     // true if this thread is part of a threadpool (and can run arbitrary tasks)
        public mi_memid_t memid;          // provenance of the tld memory itself (meta or OS)
    }

    // ------------------------------------------------------
    // Arenas
    // ------------------------------------------------------

    // A memory arena
    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct mi_arena_t
    {
        public mi_memid_t memid;          // provenance of the memory area
        public mi_subproc_t* subproc;     // subprocess this arena belongs to
        public nuint arena_idx;           // index in the arenas array

        public nuint slice_count;         // total size of the area in arena slices (of `MI_ARENA_SLICE_SIZE`)
        public nuint info_slices;         // initial slices reserved for the arena bitmaps
        public int numa_node;             // associated NUMA node
        public bool is_exclusive;         // only allow allocations if specifically for this arena
        public long purge_expire;         // atomic mi_msecs_t; expiration time when slices can be purged from `slices_purge`
        public void* commit_fun;          // custom commit/decommit function (mi_commit_fun_t*; not supported in the port)
        public void* commit_fun_arg;      // user argument for a custom commit function

        public nuint total_size;          // for (user given) memory more than MI_ARENA_MAX_SIZE, we use N arena's to cover it
        public mi_arena_t* parent;        // if this is a sub arena, this points to the first one in the memory area

        public mi_bbitmap_t* slices_free;      // is the slice free? (a binned bitmap with size classes)
        public mi_bitmap_t* slices_committed;  // is the slice committed? (i.e. accessible)
        public mi_bitmap_t* slices_dirty;      // is the slice potentially non-zero?
        public mi_bitmap_t* slices_purge;      // slices that can be purged
        public mi_page_t* pages_meta;          // pre-allocated `slice_count` page meta info (MI_PAGE_META_IS_SEPARATED == 1)
        public mi_arena_pages_t pages_main;    // arena page bitmaps for the main heap are allocated up front as well

        // followed by the bitmaps (whose sizes depend on the arena size)
        // note: when adding bitmaps revise `mi_arena_info_slices_needed`
    }

    // note: mi_bitmap_t / mi_bbitmap_t are defined in Bitmap.cs,
    // mi_meta_page_t in ArenaMeta.cs
}
