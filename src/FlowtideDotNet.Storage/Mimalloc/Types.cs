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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Mimalloc
{
    /// Type of deferred free functions.
    /// @param force If \a true all outstanding items should be freed.
    /// @param heartbeat A monotonically increasing count.
    /// @param arg Argument that was passed at registration to hold extra state.
    /// 
    /// @see mi_register_deferred_free
    public unsafe struct mi_deferred_free_fun
    {
        public delegate* managed<bool, ulong, void*, void> fun;

        public void invoke(bool force, ulong heartbeat, void* arg) => fun(force, heartbeat, arg);

        private mi_deferred_free_fun(delegate* managed<bool, ulong, void*, void> func) => this.fun = func;
        public static implicit operator mi_deferred_free_fun(delegate* managed<bool, ulong, void*, void> func) => new mi_deferred_free_fun(func);
        public static implicit operator delegate* managed<bool, ulong, void*, void>(mi_deferred_free_fun func) => func.fun;
    }

    /// Type of output functions.
    /// @param msg Message to output.
    /// @param arg Argument that was passed at registration to hold extra state.
    /// 
    /// @see mi_register_output()
    public unsafe struct mi_output_fun
    {
        public delegate* managed<byte*, void*, void> fun;

        public void invoke(byte* msg, void* arg) => fun(msg, arg);

        private mi_output_fun(delegate* managed<byte*, void*, void> func) => this.fun = func;
        public static implicit operator mi_output_fun(delegate* managed<byte*, void*, void> func) => new mi_output_fun(func);
        public static implicit operator delegate* managed<byte*, void*, void>(mi_output_fun func) => func.fun;
    }

    /// Type of error callback functions.
    /// @param err Error code (see mi_register_error() for a complete list).
    /// @param arg Argument that was passed at registration to hold extra state.
    /// 
    /// @see mi_register_error()
    public unsafe struct mi_error_fun
    {
        public delegate* managed<int, void*, void> fun;

        public void invoke(int err, void* arg) => fun(err, arg);

        private mi_error_fun(delegate* managed<int, void*, void> func) => this.fun = func;
        public static implicit operator mi_error_fun(delegate* managed<int, void*, void> func) => new mi_error_fun(func);
        public static implicit operator delegate* managed<int, void*, void>(mi_error_fun func) => func.fun;
    }

    /// Mimalloc uses large (virtual) memory areas, called "arena"s, from the OS to manage its memory.
    /// Each arena has an associated identifier.
    public struct mi_arena_id_t
    {
        public int value;

        private mi_arena_id_t(int value) => this.value = value;
        public static implicit operator mi_arena_id_t(int value) => new mi_arena_id_t(value);
        public static implicit operator int(mi_arena_id_t value) => value.value;
    }

    /// A process can associate threads with sub-processes.
    /// A sub-process will not reclaim memory from (abandoned heaps/threads)
    /// other subprocesses.
    public unsafe struct mi_subproc_id_t
    {
        public void* value;

        private mi_subproc_id_t(void* value) => this.value = value;
        public static implicit operator mi_subproc_id_t(void* value) => new mi_subproc_id_t(value);
        public static implicit operator void*(mi_subproc_id_t value) => value.value;
    }

    /// Pointer of type of first-class heaps.
    /// A heap can only be used for allocation in
    /// the thread that created this heap! Any allocated
    /// blocks can be freed or reallocated by any other thread though.
    public unsafe struct mi_heap_t_ptr
    {
        public void* ptr;

        private mi_heap_t_ptr(void* ptr) => this.ptr = ptr;
        public static implicit operator mi_heap_t_ptr(void* ptr) => new mi_heap_t_ptr(ptr);
        public static implicit operator void*(mi_heap_t_ptr ptr) => ptr.ptr;
    }

    /// Pointer of an area of heap space contains blocks of a single size.
    /// The bytes in freed blocks are `committed - used`.
    public unsafe struct mi_heap_area_t_ptr
    {
        public void* ptr;

        private mi_heap_area_t_ptr(void* ptr) => this.ptr = ptr;
        public static implicit operator mi_heap_area_t_ptr(void* ptr) => new mi_heap_area_t_ptr(ptr);
        public static implicit operator void*(mi_heap_area_t_ptr ptr) => ptr.ptr;
    }

    /// Visitor function passed to mi_heap_visit_blocks()
    /// @returns \a true if ok, \a false to stop visiting (i.e. break)
    /// 
    /// This function is always first called for every \a area
    /// with \a block as a \a NULL pointer. If \a visit_all_blocks
    /// was \a true, the function is then called for every allocated
    /// block in that area.
    public unsafe struct mi_block_visit_fun
    {
        public delegate* managed<mi_heap_t_ptr, mi_heap_area_t_ptr, void*, nuint, void*, bool> fun;

        public bool invoke(mi_heap_t_ptr heap, mi_heap_area_t_ptr area, void* block, nuint block_size, void* arg) => fun(heap, area, block, block_size, arg);

        private mi_block_visit_fun(delegate* managed<mi_heap_t_ptr, mi_heap_area_t_ptr, void*, nuint, void*, bool> func) => this.fun = func;
        public static implicit operator mi_block_visit_fun(delegate* managed<mi_heap_t_ptr, mi_heap_area_t_ptr, void*, nuint, void*, bool> func) => new mi_block_visit_fun(func);
        public static implicit operator delegate* managed<mi_heap_t_ptr, mi_heap_area_t_ptr, void*, nuint, void*, bool>(mi_block_visit_fun func) => func.fun;
    }

    /// Runtime options.
    public enum mi_option_t
    {
        mi_option_show_errors,                // print error messages
        mi_option_show_stats,                 // print statistics on termination
        mi_option_verbose,                    // print verbose messages
                                              // advanced options
        mi_option_deprecated_eager_commit,
        mi_option_arena_eager_commit,         // eager commit arenas? Use 2 to enable just on overcommit systems (=2)
        mi_option_purge_decommits,            // should a memory purge decommit? (=1). Set to 0 to use memory reset on a purge (instead of decommit)
        mi_option_allow_large_os_pages,       // allow use of large (2 or 4 MiB) OS pages, implies eager commit.
        mi_option_reserve_huge_os_pages,      // reserve N huge OS pages (1GiB pages) at startup
        mi_option_reserve_huge_os_pages_at,   // reserve huge OS pages at a specific NUMA node
        mi_option_reserve_os_memory,          // reserve specified amount of OS memory in an arena at startup (internally, this value is in KiB; use `mi_option_get_size`)
        mi_option_deprecated_segment_cache,
        mi_option_deprecated_page_reset,
        mi_option_deprecated_abandoned_page_purge,
        mi_option_deprecated_segment_reset,
        mi_option_deprecated_eager_commit_delay,
        mi_option_purge_delay,                // memory purging is delayed by N milli seconds; use 0 for immediate purging or -1 for no purging at all. (=10)
        mi_option_use_numa_nodes,             // 0 = use all available numa nodes, otherwise use at most N nodes.
        mi_option_disallow_os_alloc,          // 1 = do not use OS memory for allocation (but only programmatically reserved arenas)
        mi_option_os_tag,                     // tag used for OS logging (macOS only for now) (=100)
        mi_option_max_errors,                 // issue at most N error messages
        mi_option_max_warnings,               // issue at most N warning messages
        mi_option_deprecated_max_segment_reclaim,  // max. percentage of the abandoned segments can be reclaimed per try (=10%)
        mi_option_destroy_on_exit,            // if set, release all memory on exit; sometimes used for dynamic unloading but can be unsafe
        mi_option_arena_reserve,              // initial memory size for arena reservation (= 1 GiB on 64-bit) (internally, this value is in KiB; use `mi_option_get_size`)
        mi_option_arena_purge_mult,           // multiplier for `purge_delay` for the purging delay for arenas (=10)
        mi_option_deprecated_purge_extend_delay,
        mi_option_disallow_arena_alloc,       // 1 = do not use arena's for allocation (except if using specific arena id's)
        mi_option_retry_on_oom,               // retry on out-of-memory for N milli seconds (=400), set to 0 to disable retries. (only on windows)
        mi_option_visit_abandoned,            // allow visiting theap blocks from abandoned threads (=0)
        mi_option_guarded_min,                // only used when building with MI_GUARDED: minimal rounded object size for guarded objects (=0)
        mi_option_guarded_max,                // only used when building with MI_GUARDED: maximal rounded object size for guarded objects (=0)
        mi_option_guarded_precise,            // disregard minimal alignment requirement to always place guarded blocks exactly in front of a guard page (=0)
        mi_option_guarded_sample_rate,        // 1 out of N allocations in the min/max range will be guarded (=1000)
        mi_option_guarded_sample_seed,        // can be set to allow for a (more) deterministic re-execution when a guard page is triggered (=0)
        mi_option_generic_collect,            // collect theaps every N (=10000) generic allocation calls
        mi_option_page_reclaim_on_free,       // reclaim abandoned pages on a free (=0). -1 disallowr always, 0 allows if the page originated from the current theap, 1 allow always
        mi_option_page_full_retain,           // retain N full (small) pages per size class (=2)
        mi_option_page_max_candidates,        // max candidate pages to consider for allocation (=4)
        mi_option_max_vabits,                 // max user space virtual address bits to consider (=48)
        mi_option_pagemap_commit,             // commit the full pagemap (to always catch invalid pointer uses) (=0)
        mi_option_page_commit_on_demand,      // commit page memory on-demand
        mi_option_page_max_reclaim,           // don't reclaim pages of the same originating theap if we already own N pages (in that size class) (=-1 (unlimited))
        mi_option_page_cross_thread_max_reclaim, // don't reclaim pages across threads if we already own N pages (in that size class) (=16)
        mi_option_allow_thp,                  // allow transparent huge pages? (=1) (on Android =0 by default). Set to 0 to disable THP for the process.
        mi_option_minimal_purge_size,         // set minimal purge size (in KiB) (=0). By default set to either 64 or 2048 if THP is enabled.
        mi_option_arena_max_object_size,      // set maximal object size that can be allocated in an arena (in KiB) (=2GiB on 64-bit). 
        mi_option_arena_is_numa_local,        // experimental
        _mi_option_last,
        // legacy option names
        mi_option_large_os_pages = mi_option_allow_large_os_pages,
        mi_option_eager_region_commit = mi_option_arena_eager_commit,
        mi_option_reset_decommits = mi_option_purge_decommits,
        mi_option_reset_delay = mi_option_purge_delay,
        mi_option_limit_os_alloc = mi_option_disallow_os_alloc
    }
}
