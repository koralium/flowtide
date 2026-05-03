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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Mimalloc
{
    /// <summary>
    ///     https://github.com/microsoft/mimalloc
    /// </summary>
    [SuppressUnmanagedCodeSecurity]
    public static unsafe class MiMalloc
    {
        internal const string NATIVE_LIBRARY = "mimalloc";

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mi_version();

        // \defgroup malloc Basic Allocation
        // The basic allocation interface.

        /// Free previously allocated memory.
        /// The pointer `p` must have been allocated before (or be \a NULL).
        /// @param p  pointer to free, or \a NULL.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_free(void* p);

        /// Allocate \a size bytes.
        /// @param size  number of bytes to allocate.
        /// @returns pointer to the allocated memory or \a NULL if out of memory.
        /// Returns a unique pointer if called with \a size 0.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_malloc(nuint size);

        /// Allocate zero-initialized `size` bytes.
        /// @param size The size in bytes.
        /// @returns Pointer to newly allocated zero initialized memory,
        /// or \a NULL if out of memory.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_zalloc(nuint size);

        /// Allocate zero-initialized \a count elements of \a size bytes.
        /// @param count number of elements.
        /// @param size  size of each element.
        /// @returns pointer to the allocated memory
        /// of \a size*\a count bytes, or \a NULL if either out of memory
        /// or when `count*size` overflows.
        /// 
        /// Returns a unique pointer if called with either \a size or \a count of 0.
        /// @see mi_zalloc()
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_calloc(nuint count, nuint size);

        /// Re-allocate memory to \a newsize bytes.
        /// @param p  pointer to previously allocated memory (or \a NULL).
        /// @param newsize  the new required size in bytes.
        /// @returns pointer to the re-allocated memory
        /// of \a newsize bytes, or \a NULL if out of memory.
        /// If \a NULL is returned, the pointer \a p is not freed.
        /// Otherwise the original pointer is either freed or returned
        /// as the reallocated result (in case it fits in-place with the
        /// new size). If the pointer \a p is \a NULL, it behaves as
        /// \a mi_malloc(\a newsize). If \a newsize is larger than the
        /// original \a size allocated for \a p, the bytes after \a size
        /// are uninitialized.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_realloc(void* p, nuint newsize);

        /// Re-allocate memory to \a count elements of \a size bytes, with extra memory initialized to zero.
        /// @param p Pointer to a previously allocated block (or \a NULL).
        /// @param count The number of elements.
        /// @param size The size of each element.
        /// @returns A pointer to a re-allocated block of \a count * \a size bytes, or \a NULL
        /// if out of memory or if \a count * \a size overflows.
        /// 
        /// If there is no overflow, it behaves exactly like `mi_rezalloc(p,count*size)`.
        /// @see mi_reallocn()
        /// @see [reallocarray()](http://man.openbsd.org/reallocarray) (on BSD)
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_recalloc(void* p, nuint count, nuint size);

        /// Try to re-allocate memory to \a newsize bytes _in place_.
        /// @param p  pointer to previously allocated memory (or \a NULL).
        /// @param newsize  the new required size in bytes.
        /// @returns pointer to the re-allocated memory
        /// of \a newsize bytes (always equal to \a p),
        /// or \a NULL if either out of memory or if
        /// the memory could not be expanded in place.
        /// If \a NULL is returned, the pointer \a p is not freed.
        /// Otherwise the original pointer is returned
        /// as the reallocated result since it fits in-place with the
        /// new size. If \a newsize is larger than the
        /// original \a size allocated for \a p, the bytes after \a size
        /// are uninitialized.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_expand(void* p, nuint newsize);

        /// Allocate \a count elements of \a size bytes.
        /// @param count The number of elements.
        /// @param size The size of each element.
        /// @returns A pointer to a block of \a count * \a size bytes, or \a NULL
        /// if out of memory or if \a count * \a size overflows.
        /// 
        /// If there is no overflow, it behaves exactly like `mi_malloc(count*size)`.
        /// @see mi_calloc()
        /// @see mi_zallocn()
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_mallocn(nuint count, nuint size);

        /// Re-allocate memory to \a count elements of \a size bytes.
        /// @param p Pointer to a previously allocated block (or \a NULL).
        /// @param count The number of elements.
        /// @param size The size of each element.
        /// @returns A pointer to a re-allocated block of \a count * \a size bytes, or \a NULL
        /// if out of memory or if \a count * \a size overflows.
        /// 
        /// If there is no overflow, it behaves exactly like `mi_realloc(p,count*size)`.
        /// @see [reallocarray()](http:// man.openbsd.org/ reallocarray) (on BSD)
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_reallocn(void* p, nuint count, nuint size);

        /// Re-allocate memory to \a newsize bytes,
        /// @param p  pointer to previously allocated memory (or \a NULL).
        /// @param newsize  the new required size in bytes.
        /// @returns pointer to the re-allocated memory
        /// of \a newsize bytes, or \a NULL if out of memory.
        /// 
        /// In contrast to mi_realloc(), if \a NULL is returned, the original pointer
        /// \a p is freed (if it was not \a NULL itself).
        /// Otherwise the original pointer is either freed or returned
        /// as the reallocated result (in case it fits in-place with the
        /// new size). If the pointer \a p is \a NULL, it behaves as
        /// \a mi_malloc(\a newsize). If \a newsize is larger than the
        /// original \a size allocated for \a p, the bytes after \a size
        /// are uninitialized.
        /// 
        /// @see [reallocf](https://www.freebsd.org/cgi/man.cgi?query=reallocf) (on BSD)
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_reallocf(void* p, nuint newsize);


        /// Allocate and duplicate a string.
        /// @param s string to duplicate (or \a NULL).
        /// @returns a pointer to newly allocated memory initialized
        /// to string \a s, or \a NULL if either out of memory or if
        /// \a s is \a NULL.
        /// 
        /// Replacement for the standard [strdup()](http://pubs.opengroup.org/onlinepubs/9699919799/functions/strdup.html)
        /// such that mi_free() can be used on the returned result.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern byte* mi_strdup(byte* s);

        /// Allocate and duplicate a string up to \a n bytes.
        /// @param s string to duplicate (or \a NULL).
        /// @param n maximum number of bytes to copy (excluding the terminating zero).
        /// @returns a pointer to newly allocated memory initialized
        /// to string \a s up to the first \a n bytes (and always zero terminated),
        /// or \a NULL if either out of memory or if \a s is \a NULL.
        /// 
        /// Replacement for the standard [strndup()](http://pubs.opengroup.org/onlinepubs/9699919799/functions/strndup.html)
        /// such that mi_free() can be used on the returned result.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern byte* mi_strndup(byte* s, nuint n);

        /// Resolve a file path name.
        /// @param fname File name.
        /// @param resolved_name Should be \a NULL (but can also point to a buffer
        /// of at least \a PATH_MAX bytes).
        /// @returns If successful a pointer to the resolved absolute file name, or
        /// \a NULL on failure (with \a errno set to the error code).
        /// 
        /// If \a resolved_name was \a NULL, the returned result should be freed with
        /// mi_free().
        /// 
        /// Replacement for the standard [realpath()](http://pubs.opengroup.org/onlinepubs/9699919799/functions/realpath.html)
        /// such that mi_free() can be used on the returned result (if \a resolved_name was \a NULL).
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern byte* mi_realpath(byte* fname, byte* resolved_name);

        // \defgroup extended Extended Functions
        // Extended functionality.

        public static readonly nuint MI_SMALL_SIZE_MAX = (nuint)(128 * sizeof(void*));

        /// Allocate a small object.
        /// @param size The size in bytes, can be at most #MI_SMALL_SIZE_MAX.
        /// @returns a pointer to newly allocated memory of at least \a size
        /// bytes, or \a NULL if out of memory.
        /// This function is meant for use in run-time systems for best
        /// performance and does not check if \a size was indeed small -- use
        /// with care!
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_malloc_small(nuint size);

        /// Allocate a zero initialized small object.
        /// @param size The size in bytes, can be at most #MI_SMALL_SIZE_MAX.
        /// @returns a pointer to newly allocated zero-initialized memory of at
        /// least \a size bytes, or \a NULL if out of memory.
        /// This function is meant for use in run-time systems for best
        /// performance and does not check if \a size was indeed small -- use
        /// with care!
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_zalloc_small(nuint size);

        /// Return the available bytes in a memory block.
        /// @param p Pointer to previously allocated memory (or \a NULL)
        /// @returns Returns the available bytes in the memory block, or
        /// 0 if \a p was \a NULL.
        /// 
        /// The returned size can be
        /// used to call \a mi_expand successfully.
        /// The returned size is always at least equal to the
        /// allocated size of \a p.
        /// 
        /// @see [_msize](https://docs.microsoft.com/en-us/cpp/c-runtime-library/reference/msize?view=vs-2017) (Windows)
        /// @see [malloc_usable_size](http://man7.org/linux/man-pages/man3/malloc_usable_size.3.html) (Linux)
        /// @see mi_good_size()
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern nuint mi_usable_size(void* p);

        /// Return the used allocation size.
        /// @param size The minimal required size in bytes.
        /// @returns the size `n` that will be allocated, where `n >= size`.
        /// 
        /// Generally, `mi_usable_size(mi_malloc(size)) == mi_good_size(size)`.
        /// This can be used to reduce internal wasted space when
        /// allocating buffers for example.
        /// 
        /// @see mi_usable_size()
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern nuint mi_good_size(nuint size);

        /// Eagerly free memory.
        /// @param force If \a true, aggressively return memory to the OS (can be expensive!)
        /// 
        /// Regular code should not have to call this function. It can be beneficial
        /// in very narrow circumstances; in particular, when a long running thread
        /// allocates a lot of blocks that are freed by other threads it may improve
        /// resource usage by calling this every once in a while.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_collect(bool force);

        /// Deprecated
        /// @param out Ignored, outputs to the registered output function or stderr by default.
        /// 
        /// Most detailed when using a debug build.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_stats_print(void* @ut);

        /// Print the main statistics.
        /// @param out An output function or \a NULL for the default.
        /// @param arg Optional argument passed to \a out (if not \a NULL)
        /// 
        /// Most detailed when using a debug build.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_stats_print_out(mi_output_fun* @out, void* arg);

        /// Reset statistics.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_stats_reset();

        /// Merge thread local statistics with the main statistics and reset.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_stats_merge();

        /// Initialize mimalloc on a thread.
        /// Should not be used as on most systems (pthreads, windows) this is done
        /// automatically.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_thread_init();

        /// Uninitialize mimalloc on a thread.
        /// Should not be used as on most systems (pthreads, windows) this is done
        /// automatically. Ensures that any memory that is not freed yet (but will
        /// be freed by other threads in the future) is properly handled.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_thread_done();

        /// Print out heap statistics for this thread.
        /// @param out An output function or \a NULL for the default.
        /// @param arg Optional argument passed to \a out (if not \a NULL)
        /// 
        /// Most detailed when using a debug build.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_thread_stats_print_out(mi_output_fun* @out, void* arg);

        /// Register a deferred free function.
        /// @param deferred_free Address of a deferred free-ing function or \a NULL to unregister.
        /// @param arg Argument that will be passed on to the deferred free function.
        /// 
        /// Some runtime systems use deferred free-ing, for example when using
        /// reference counting to limit the worst case free time.
        /// Such systems can register (re-entrant) deferred free function
        /// to free more memory on demand. When the \a force parameter is
        /// \a true all possible memory should be freed.
        /// The per-thread \a heartbeat parameter is monotonically increasing
        /// and guaranteed to be deterministic if the program allocates
        /// deterministically. The \a deferred_free function is guaranteed
        /// to be called deterministically after some number of allocations
        /// (regardless of freeing or available free memory).
        /// At most one \a deferred_free function can be active.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_register_deferred_free(mi_deferred_free_fun* deferred_free, void* arg);

        /// Register an output function.
        /// @param out The output function, use `NULL` to output to stderr.
        /// @param arg Argument that will be passed on to the output function.
        /// 
        /// The `out` function is called to output any information from mimalloc,
        /// like verbose or warning messages.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_register_output(mi_output_fun* @out, void* arg);

        /// Register an error callback function.
        /// @param errfun The error function that is called on an error (use \a NULL for default)
        /// @param arg Extra argument that will be passed on to the error function.
        /// 
        /// The \a errfun function is called on an error in mimalloc after emitting
        /// an error message (through the output function). It as always legal to just
        /// return from the \a errfun function in which case allocation functions generally
        /// return \a NULL or ignore the condition. The default function only calls abort()
        /// when compiled in secure mode with an \a EFAULT error. The possible error
        /// codes are:
        /// * \a EAGAIN: Double free was detected (only in debug and secure mode).
        /// * \a EFAULT: Corrupted free list or meta-data was detected (only in debug and secure mode).
        /// * \a ENOMEM: Not enough memory available to satisfy the request.
        /// * \a EOVERFLOW: Too large a request, for example in mi_calloc(), the \a count and \a size parameters are too large.
        /// * \a EINVAL: Trying to free or re-allocate an invalid pointer.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_register_error(mi_error_fun* errfun, void* arg);

        /// Is a pointer part of our heap?
        /// @param p The pointer to check.
        /// @returns \a true if this is a pointer into our heap.
        /// This function is relatively fast.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern bool mi_is_in_heap_region(void* p);

        /// Reserve OS memory for use by mimalloc. Reserved areas are used
        /// before allocating from the OS again. By reserving a large area upfront,
        /// allocation can be more efficient, and can be better managed on systems
        /// without `mmap`/`VirtualAlloc` (like WASM for example).
        /// @param size        The size to reserve.
        /// @param commit      Commit the memory upfront.
        /// @param allow_large Allow large OS pages (2MiB) to be used?
        /// @return \a 0 if successful, and an error code otherwise (e.g. `ENOMEM`).
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mi_reserve_os_memory(nuint size, bool commit, bool allow_large);

        /// Manage a particular memory area for use by mimalloc.
        /// This is just like `mi_reserve_os_memory` except that the area should already be
        /// allocated in some manner and available for use my mimalloc.
        /// @param start       Start of the memory area
        /// @param size        The size of the memory area.
        /// @param is_committed Is the area already committed?
        /// @param is_pinned   Can the memory not be decommitted or reset? (usually the case for large OS pages)
        /// @param is_zero     Does the area consists of zero's?
        /// @param numa_node   Possible associated numa node or `-1`.
        /// @return \a true if successful, and \a false on error.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern bool mi_manage_os_memory(void* start, nuint size, bool is_committed, bool is_pinned, bool is_zero, int numa_node);

        /// Reserve \a pages of huge OS pages (1GiB) evenly divided over \a numa_nodes nodes,
        /// but stops after at most `timeout_msecs` seconds.
        /// @param pages The number of 1GiB pages to reserve.
        /// @param numa_nodes The number of nodes do evenly divide the pages over, or 0 for using the actual number of NUMA nodes.
        /// @param timeout_msecs Maximum number of milli-seconds to try reserving, or 0 for no timeout.
        /// @returns 0 if successful, \a ENOMEM if running out of memory, or \a ETIMEDOUT if timed out.
        /// 
        /// The reserved memory is used by mimalloc to satisfy allocations.
        /// May quit before \a timeout_msecs are expired if it estimates it will take more than
        /// 1.5 times \a timeout_msecs. The time limit is needed because on some operating systems
        /// it can take a long time to reserve contiguous memory if the physical memory is
        /// fragmented.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mi_reserve_huge_os_pages_interleave(nuint pages, nuint numa_nodes, nuint timeout_msecs);

        /// Reserve \a pages of huge OS pages (1GiB) at a specific \a numa_node,
        /// but stops after at most `timeout_msecs` seconds.
        /// @param pages The number of 1GiB pages to reserve.
        /// @param numa_node The NUMA node where the memory is reserved (start at 0). Use -1 for no affinity.
        /// @param timeout_msecs Maximum number of milli-seconds to try reserving, or 0 for no timeout.
        /// @returns 0 if successful, \a ENOMEM if running out of memory, or \a ETIMEDOUT if timed out.
        /// 
        /// The reserved memory is used by mimalloc to satisfy allocations.
        /// May quit before \a timeout_msecs are expired if it estimates it will take more than
        /// 1.5 times \a timeout_msecs. The time limit is needed because on some operating systems
        /// it can take a long time to reserve contiguous memory if the physical memory is
        /// fragmented.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mi_reserve_huge_os_pages_at(nuint pages, int numa_node, nuint timeout_msecs);


        /// Is the C runtime \a malloc API redirected?
        /// @returns \a true if all malloc API calls are redirected to mimalloc.
        /// 
        /// Currently only used on Windows.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern bool mi_is_redirected();

        /// Return process information (time and memory usage).
        /// @param elapsed_msecs   Optional. Elapsed wall-clock time of the process in milli-seconds.
        /// @param user_msecs      Optional. User time in milli-seconds (as the sum over all threads).
        /// @param system_msecs    Optional. System time in milli-seconds.
        /// @param current_rss     Optional. Current working set size (touched pages).
        /// @param peak_rss        Optional. Peak working set size (touched pages).
        /// @param current_commit  Optional. Current committed memory (backed by the page file).
        /// @param peak_commit     Optional. Peak committed memory (backed by the page file).
        /// @param page_faults     Optional. Count of hard page faults.
        /// 
        /// The \a current_rss is precise on Windows and MacOSX; other systems estimate
        /// this using \a current_commit. The \a commit is precise on Windows but estimated
        /// on other systems as the amount of read/write accessible memory reserved by mimalloc.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_process_info(nuint* elapsed_msecs, nuint* user_msecs, nuint* system_msecs, nuint* current_rss, nuint* peak_rss, nuint* current_commit, nuint* peak_commit, nuint* page_faults);

        /// @brief Show all current arena's.
        /// @param show_inuse       Show the arena blocks that are in use.
        /// @param show_abandoned   Show the abandoned arena blocks.
        /// @param show_purge       Show arena blocks scheduled for purging.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_debug_show_arenas(bool show_inuse, bool show_abandoned, bool show_purge);

        /// @brief  Return the size of an arena.
        /// @param arena_id  The arena identifier.
        /// @param size      Returned size in bytes of the (virtual) arena area.
        /// @return base address of the arena.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_arena_area(mi_arena_id_t arena_id, nuint* size);

        /// @brief Reserve huge OS pages (1GiB) into a single arena.
        /// @param pages             Number of 1GiB pages to reserve.
        /// @param numa_node         The associated NUMA node, or -1 for no NUMA preference.
        /// @param timeout_msecs     Max amount of milli-seconds this operation is allowed to take. (0 is infinite)
        /// @param exclusive         If exclusive, only a heap associated with this arena can allocate in it.
        /// @param arena_id          The arena identifier.
        /// @return 0 if successful, \a ENOMEM if running out of memory, or \a ETIMEDOUT if timed out.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mi_reserve_huge_os_pages_at_ex(nuint pages, int numa_node, nuint timeout_msecs, bool exclusive, mi_arena_id_t* arena_id);

        /// @brief Reserve OS memory to be managed in an arena.
        /// @param size Size the reserve.
        /// @param commit Should the memory be initially committed?
        /// @param allow_large Allow the use of large OS pages?
        /// @param exclusive  Is the returned arena exclusive?
        /// @param arena_id The new arena identifier.
        /// @return Zero on success, an error code otherwise.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mi_reserve_os_memory_ex(nuint size, bool commit, bool allow_large, bool exclusive, mi_arena_id_t* arena_id);

        /// @brief Manage externally allocated memory as a mimalloc arena. This memory will not be freed by mimalloc.
        /// @param start Start address of the area.
        /// @param size  Size in bytes of the area.
        /// @param is_committed  Is the memory already committed?
        /// @param is_large      Does it consist of (pinned) large OS pages?
        /// @param is_zero       Is the memory zero-initialized?
        /// @param numa_node     Associated NUMA node, or -1 to have no NUMA preference.
        /// @param exclusive     Is the arena exclusive (where only heaps associated with the arena can allocate in it)
        /// @param arena_id      The new arena identifier.
        /// @return `true` if successful.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern bool mi_manage_os_memory_ex(void* start, nuint size, bool is_committed, bool is_large, bool is_zero, int numa_node, bool exclusive, mi_arena_id_t* arena_id);

        /// @brief Create a new heap that only allocates in the specified arena.
        /// @param arena_id The arena identifier.
        /// @return The new heap or `NULL`.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern mi_heap_t_ptr mi_heap_new_in_arena(mi_arena_id_t arena_id);

        /// @brief Create a new heap
        /// @param heap_tag       The heap tag associated with this heap; heaps only reclaim memory between heaps with the same tag.
        /// @param allow_destroy  Is \a mi_heap_destroy allowed?  Not allowing this allows the heap to reclaim memory from terminated threads.
        /// @param arena_id       If not 0, the heap will only allocate from the specified arena.
        /// @return A new heap or `NULL` on failure.
        /// 
        /// The \a arena_id can be used by runtimes to allocate only in a specified pre-reserved arena.
        /// This is used for example for a compressed pointer heap in Koka.
        /// The \a heap_tag enables heaps to keep objects of a certain type isolated to heaps with that tag.
        /// This is used for example in the CPython integration.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern mi_heap_t_ptr mi_heap_new_ex(int heap_tag, bool allow_destroy, mi_arena_id_t arena_id);

        /// @brief  Get the main sub-process identifier.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern mi_subproc_id_t mi_subproc_main();

        /// @brief Create a fresh sub-process (with no associated threads yet).
        /// @return The new sub-process identifier.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern mi_subproc_id_t mi_subproc_new();

        /// @brief Delete a previously created sub-process.
        /// @param subproc The sub-process identifier.
        /// Only delete sub-processes if all associated threads have terminated.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_subproc_delete(mi_subproc_id_t subproc);

        /// Add the current thread to the given sub-process.
        /// This should be called right after a thread is created (and no allocation has taken place yet)
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_subproc_add_current_thread(mi_subproc_id_t subproc);


        // ------------------------------------------------------
        // Aligned allocation
        // ------------------------------------------------------

        // \defgroup aligned Aligned Allocation
        //
        // Allocating aligned memory blocks.
        // Note that `alignment` always follows `size` for consistency with the unaligned
        // allocation API, but unfortunately this differs from `posix_memalign` and `aligned_alloc` in the C library.

        // Allocate \a size bytes aligned by \a alignment.
        // @param size  number of bytes to allocate.
        // @param alignment  the minimal alignment of the allocated memory.
        // @returns pointer to the allocated memory or \a NULL if out of memory,
        // or if the alignment is not a power of 2 (including 0). The \a size is unrestricted
        // (and does not have to be an integral multiple of the \a alignment).
        // The returned pointer is aligned by \a alignment, i.e. `(uintptr_t)p % alignment == 0`.
        // Returns a unique pointer if called with \a size 0.
        // 
        // Note that `alignment` always follows `size` for consistency with the unaligned
        // allocation API, but unfortunately this differs from `posix_memalign` and `aligned_alloc` in the C library.
        // 
        // @see [aligned_alloc](https://en.cppreference.com/w/c/memory/aligned_alloc) (in the standard C11 library, with switched arguments!)
        // @see [_aligned_malloc](https://docs.microsoft.com/en-us/cpp/c-runtime-library/reference/aligned-malloc?view=vs-2017) (on Windows)
        // @see [aligned_alloc](http://man.openbsd.org/reallocarray) (on BSD, with switched arguments!)
        // @see [posix_memalign](https://linux.die.net/man/3/posix_memalign) (on Posix, with switched arguments!)
        // @see [memalign](https://linux.die.net/man/3/posix_memalign) (on Linux, with switched arguments!)

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_malloc_aligned(nuint size, nuint alignment);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_zalloc_aligned(nuint size, nuint alignment);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_calloc_aligned(nuint count, nuint size, nuint alignment);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_realloc_aligned(void* p, nuint newsize, nuint alignment);

        // Allocate \a size bytes aligned by \a alignment at a specified \a offset.
        // @param size  number of bytes to allocate.
        // @param alignment  the minimal alignment of the allocated memory at \a offset.
        // @param offset     the offset that should be aligned.
        // @returns pointer to the allocated memory or \a NULL if out of memory,
        // or if the alignment is not a power of 2 (including 0). The \a size is unrestricted
        // (and does not have to be an integral multiple of the \a alignment).
        // The returned pointer is aligned by \a alignment, i.e. `(uintptr_t)p % alignment == 0`.
        // Returns a unique pointer if called with \a size 0.
        // 
        // @see [_aligned_offset_malloc](https://docs.microsoft.com/en-us/cpp/c-runtime-library/reference/aligned-offset-malloc?view=vs-2017) (on Windows)

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_malloc_aligned_at(nuint size, nuint alignment, nuint offset);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_zalloc_aligned_at(nuint size, nuint alignment, nuint offset);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_calloc_aligned_at(nuint count, nuint size, nuint alignment, nuint offset);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_realloc_aligned_at(void* p, nuint newsize, nuint alignment, nuint offset);

        // \defgroup heap Heap Allocation
        //
        // First-class heaps that can be destroyed in one go.

        /// Create a new heap that can be used for allocation.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern mi_heap_t_ptr mi_heap_new();

        /// Delete a previously allocated heap.
        /// This will release resources and migrate any
        /// still allocated blocks in this heap (efficiently)
        /// to the default heap.
        /// 
        /// If \a heap is the default heap, the default
        /// heap is set to the backing heap.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_heap_delete(mi_heap_t_ptr heap);

        /// Destroy a heap, freeing all its still allocated blocks.
        /// Use with care as this will free all blocks still
        /// allocated in the heap. However, this can be a very
        /// efficient way to free all heap memory in one go.
        /// 
        /// If \a heap is the default heap, the default
        /// heap is set to the backing heap.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_heap_destroy(mi_heap_t_ptr heap);

        /// Set the default heap to use in the current thread for mi_malloc() et al.
        /// @param heap  The new default heap.
        /// @returns The previous default heap.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern mi_heap_t_ptr mi_heap_set_default(mi_heap_t_ptr heap);

        /// Get the default heap that is used for mi_malloc() et al. (for the current thread).
        /// @returns The current default heap.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern mi_heap_t_ptr mi_heap_get_default();

        /// Get the backing heap.
        /// The _backing_ heap is the initial default heap for
        /// a thread and always available for allocations.
        /// It cannot be destroyed or deleted
        /// except by exiting the thread.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern mi_heap_t_ptr mi_heap_get_backing();

        /// Release outstanding resources in a specific heap.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_heap_collect(mi_heap_t_ptr heap, bool force);

        /// Allocate in a specific heap.
        /// @see mi_malloc()
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_malloc(mi_heap_t_ptr heap, nuint size);

        /// Allocate a small object in a specific heap.
        /// \a size must be smaller or equal to MI_SMALL_SIZE_MAX().
        /// @see mi_malloc()
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_malloc_small(mi_heap_t_ptr heap, nuint size);

        /// Allocate zero-initialized in a specific heap.
        /// @see mi_zalloc()
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_zalloc(mi_heap_t_ptr heap, nuint size);

        /// Allocate \a count zero-initialized elements in a specific heap.
        /// @see mi_calloc()
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_calloc(mi_heap_t_ptr heap, nuint count, nuint size);

        /// Allocate \a count elements in a specific heap.
        /// @see mi_mallocn()
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_mallocn(mi_heap_t_ptr heap, nuint count, nuint size);

        /// Duplicate a string in a specific heap.
        /// @see mi_strdup()
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern byte* mi_heap_strdup(mi_heap_t_ptr heap, byte* s);

        /// Duplicate a string of at most length \a n in a specific heap.
        /// @see mi_strndup()
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern byte* mi_heap_strndup(mi_heap_t_ptr heap, byte* s, nuint n);

        /// Resolve a file path name using a specific \a heap to allocate the result.
        /// @see mi_realpath()
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern byte* mi_heap_realpath(mi_heap_t_ptr heap, byte* fname, byte* resolved_name);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_realloc(mi_heap_t_ptr heap, void* p, nuint newsize);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_reallocn(mi_heap_t_ptr heap, void* p, nuint count, nuint size);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_reallocf(mi_heap_t_ptr heap, void* p, nuint newsize);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_malloc_aligned(mi_heap_t_ptr heap, nuint size, nuint alignment);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_malloc_aligned_at(mi_heap_t_ptr heap, nuint size, nuint alignment, nuint offset);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_zalloc_aligned(mi_heap_t_ptr heap, nuint size, nuint alignment);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_zalloc_aligned_at(mi_heap_t_ptr heap, nuint size, nuint alignment, nuint offset);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_calloc_aligned(mi_heap_t_ptr heap, nuint count, nuint size, nuint alignment);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_calloc_aligned_at(mi_heap_t_ptr heap, nuint count, nuint size, nuint alignment, nuint offset);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_realloc_aligned(mi_heap_t_ptr heap, void* p, nuint newsize, nuint alignment);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_realloc_aligned_at(mi_heap_t_ptr heap, void* p, nuint newsize, nuint alignment, nuint offset);

        // \defgroup zeroinit Zero initialized re-allocation
        //
        // The zero-initialized re-allocations are only valid on memory that was
        // originally allocated with zero initialization too.
        // e.g. `mi_calloc`, `mi_zalloc`, `mi_zalloc_aligned` etc.
        // see https://github.com/microsoft/mimalloc/issues/63#issuecomment-508272992

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_rezalloc(void* p, nuint newsize);

        // public static extern void* mi_recalloc(void* p, nuint newcount, nuint size);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_rezalloc_aligned(void* p, nuint newsize, nuint alignment);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_rezalloc_aligned_at(void* p, nuint newsize, nuint alignment, nuint offset);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_recalloc_aligned(void* p, nuint newcount, nuint size, nuint alignment);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_recalloc_aligned_at(void* p, nuint newcount, nuint size, nuint alignment, nuint offset);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_rezalloc(mi_heap_t_ptr heap, void* p, nuint newsize);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_recalloc(mi_heap_t_ptr heap, void* p, nuint newcount, nuint size);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_rezalloc_aligned(mi_heap_t_ptr heap, void* p, nuint newsize, nuint alignment);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_rezalloc_aligned_at(mi_heap_t_ptr heap, void* p, nuint newsize, nuint alignment, nuint offset);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_recalloc_aligned(mi_heap_t_ptr heap, void* p, nuint newcount, nuint size, nuint alignment);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_heap_recalloc_aligned_at(mi_heap_t_ptr heap, void* p, nuint newcount, nuint size, nuint alignment, nuint offset);

        // \defgroup typed Typed Macros
        //
        // Typed allocation macros.

        // Allocate a block of type \a tp.
        // @param tp The type of the block to allocate.
        // @returns A pointer to an object of type \a tp, or
        // \a NULL if out of memory.

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_malloc_tp<T>() where T : unmanaged => ((T*)mi_malloc((nuint)sizeof(T)));

        /// Allocate a zero-initialized block of type \a tp.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_zalloc_tp<T>() where T : unmanaged => ((T*)mi_zalloc((nuint)sizeof(T)));

        /// Allocate \a count zero-initialized blocks of type \a tp.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_calloc_tp<T>(nuint count) where T : unmanaged => ((T*)mi_calloc(count, (nuint)sizeof(T)));

        /// Allocate \a count blocks of type \a tp.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_mallocn_tp<T>(nuint count) where T : unmanaged => ((T*)mi_mallocn(count, (nuint)sizeof(T)));

        /// Re-allocate to \a count blocks of type \a tp.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_reallocn_tp<T>(void* p, nuint count) where T : unmanaged => ((T*)mi_reallocn(p, count, (nuint)sizeof(T)));

        /// Allocate a block of type \a tp in a heap \a hp.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_heap_malloc_tp<T>(mi_heap_t_ptr hp) where T : unmanaged => ((T*)mi_heap_malloc(hp, (nuint)sizeof(T)));

        /// Allocate a zero-initialized block of type \a tp in a heap \a hp.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_heap_zalloc_tp<T>(mi_heap_t_ptr hp) where T : unmanaged => ((T*)mi_heap_zalloc(hp, (nuint)sizeof(T)));

        /// Allocate \a count zero-initialized blocks of type \a tp in a heap \a hp.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_heap_calloc_tp<T>(mi_heap_t_ptr hp, nuint count) where T : unmanaged => ((T*)mi_heap_calloc(hp, count, (nuint)sizeof(T)));

        /// Allocate \a count blocks of type \a tp in a heap \a hp.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_heap_mallocn_tp<T>(mi_heap_t_ptr hp, nuint count) where T : unmanaged => ((T*)mi_heap_mallocn(hp, count, (nuint)sizeof(T)));

        /// Re-allocate to \a count blocks of type \a tp in a heap \a hp.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_heap_reallocn_tp<T>(mi_heap_t_ptr hp, void* p, nuint count) where T : unmanaged => ((T*)mi_heap_reallocn(hp, p, count, (nuint)sizeof(T)));

        /// Re-allocate to \a count zero initialized blocks of type \a tp in a heap \a hp.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T* mi_heap_recalloc_tp<T>(mi_heap_t_ptr hp, void* p, nuint count) where T : unmanaged => ((T*)mi_heap_recalloc(hp, p, count, (nuint)sizeof(T)));

        // \defgroup analysis Heap Introspection
        // 
        // Inspect the heap at runtime.

        /// Does a heap contain a pointer to a previously allocated block?
        /// @param heap The heap.
        /// @param p Pointer to a previously allocated block (in any heap)-- cannot be some
        /// random pointer!
        /// @returns \a true if the block pointed to by \a p is in the \a heap.
        /// @see mi_heap_check_owned()
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern bool mi_heap_contains_block(mi_heap_t_ptr heap, void* p);

        /// Check safely if any pointer is part of a heap.
        /// @param heap The heap.
        /// @param p   Any pointer -- not required to be previously allocated by us.
        /// @returns \a true if \a p points to a block in \a heap.
        /// 
        /// Note: expensive function, linear in the pages in the heap.
        /// @see mi_heap_contains_block()
        /// @see mi_heap_get_default()
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern bool mi_heap_check_owned(mi_heap_t_ptr heap, void* p);

        /// Check safely if any pointer is part of the default heap of this thread.
        /// @param p   Any pointer -- not required to be previously allocated by us.
        /// @returns \a true if \a p points to a block in default heap of this thread.
        /// 
        /// Note: expensive function, linear in the pages in the heap.
        /// @see mi_heap_contains_block()
        /// @see mi_heap_get_default()
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern bool mi_check_owned(void* p);

        /// Visit all areas and blocks in a heap.
        /// @param heap The heap to visit.
        /// @param visit_all_blocks If \a true visits all allocated blocks, otherwise
        /// \a visitor is only called for every heap area.
        /// @param visitor This function is called for every area in the heap
        /// (with \a block as \a NULL). If \a visit_all_blocks is
        /// \a true, \a visitor is also called for every allocated
        /// block in every area (with `block!=NULL`).
        /// return \a false from this function to stop visiting early.
        /// @param arg Extra argument passed to \a visitor.
        /// @returns \a true if all areas and blocks were visited.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern bool mi_heap_visit_blocks(mi_heap_t_ptr heap, bool visit_all_blocks, mi_block_visit_fun* visitor, void* arg);

        /// @brief Visit all areas and blocks in abandoned heaps.
        /// @param subproc_id The sub-process id associated with the abandoned heaps.
        /// @param heap_tag Visit only abandoned memory with the specified heap tag, use -1 to visit all abandoned memory.
        /// @param visit_blocks If \a true visits all allocated blocks, otherwise
        /// \a visitor is only called for every heap area.
        /// @param visitor This function is called for every area in the heap
        /// (with \a block as \a NULL). If \a visit_all_blocks is
        /// \a true, \a visitor is also called for every allocated
        /// block in every area (with `block!=NULL`).
        /// return \a false from this function to stop visiting early.
        /// @param arg extra argument passed to the \a visitor.
        /// @return \a true if all areas and blocks were visited.
        /// 
        /// Note: requires the option `mi_option_visit_abandoned` to be set
        /// at the start of the program.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern bool mi_abandoned_visit_blocks(mi_subproc_id_t subproc_id, int heap_tag, bool visit_blocks, mi_block_visit_fun* visitor, void* arg);

        // \defgroup options Runtime Options
        //
        // Set runtime behavior.

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern bool mi_option_is_enabled(mi_option_t option);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_option_enable(mi_option_t option);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_option_disable(mi_option_t option);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_option_set_enabled(mi_option_t option, bool enable);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_option_set_enabled_default(mi_option_t option, bool enable);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern long mi_option_get(mi_option_t option);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern long mi_option_get_clamp(mi_option_t option, long min, long max);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern nuint mi_option_get_size(mi_option_t option);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_option_set(mi_option_t option, long value);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_option_set_default(mi_option_t option, long value);

        // \defgroup posix Posix
        //
        //  `mi_` prefixed implementations of various Posix, Unix, and C++ allocation functions.
        //  Defined for convenience as all redirect to the regular mimalloc API.

        /// Just as `free` but also checks if the pointer `p` belongs to our heap.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_cfree(void* p);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi__expand(void* p, nuint newsize);

        // public static extern void* mi_recalloc(void* p, nuint newcount, nuint size);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern nuint mi_malloc_size(void* p);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern nuint mi_malloc_good_size(nuint size);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern nuint mi_malloc_usable_size(void* p);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mi_posix_memalign(void** p, nuint alignment, nuint size);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mi__posix_memalign(void** p, nuint alignment, nuint size);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_memalign(nuint alignment, nuint size);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_valloc(nuint size);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_pvalloc(nuint size);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_aligned_alloc(nuint alignment, nuint size);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern ushort* mi_wcsdup(ushort* s);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern byte* mi_mbsdup(byte* s);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mi_dupenv_s(byte** buf, nuint* size, byte* name);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mi_wdupenv_s(ushort** buf, nuint* size, ushort* name);

        /// Correspond s to [reallocarray](https://www.freebsd.org/cgi/man.cgi?query=reallocarray&sektion=3&manpath=freebsd-release-ports)
        /// in FreeBSD.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_reallocarray(void* p, nuint count, nuint size);

        /// Corresponds to [reallocarr](https://man.netbsd.org/reallocarr.3) in NetBSD.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mi_reallocarr(void* p, nuint count, nuint size);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_aligned_recalloc(void* p, nuint newcount, nuint size, nuint alignment);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_aligned_offset_recalloc(void* p, nuint newcount, nuint size, nuint alignment, nuint offset);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_free_size(void* p, nuint size);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_free_size_aligned(void* p, nuint size, nuint alignment);

        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mi_free_aligned(void* p, nuint alignment);

        // \defgroup cpp C++ wrappers
        //
        //  `mi_` prefixed implementations of various allocation functions
        //  that use C++ semantics on out-of-memory, generally calling
        //  `std::get_new_handler` and raising a `std::bad_alloc` exception on failure.
        //
        //  Note: use the `mimalloc-new-delete.h` header to override the \a new
        //        and \a delete operators globally. The wrappers here are mostly
        //        for convenience for library writers that need to interface with
        //        mimalloc from C++.

        /// like mi_malloc(), but when out of memory, use `std::get_new_handler` and raise `std::bad_alloc` exception on failure.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_new(nuint n);

        /// like mi_mallocn(), but when out of memory, use `std::get_new_handler` and raise `std::bad_alloc` exception on failure.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_new_n(nuint count, nuint size);

        /// like mi_malloc_aligned(), but when out of memory, use `std::get_new_handler` and raise `std::bad_alloc` exception on failure.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_new_aligned(nuint n, nuint alignment);

        /// like `mi_malloc`, but when out of memory, use `std::get_new_handler` but return \a NULL on failure.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_new_nothrow(nuint n);

        /// like `mi_malloc_aligned`, but when out of memory, use `std::get_new_handler` but return \a NULL on failure.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_new_aligned_nothrow(nuint n, nuint alignment);

        /// like mi_realloc(), but when out of memory, use `std::get_new_handler` and raise `std::bad_alloc` exception on failure.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_new_realloc(void* p, nuint newsize);

        /// like mi_reallocn(), but when out of memory, use `std::get_new_handler` and raise `std::bad_alloc` exception on failure.
        [DllImport(NATIVE_LIBRARY, CallingConvention = CallingConvention.Cdecl)]
        public static extern void* mi_new_reallocn(void* p, nuint newcount, nuint size);
    }
}
