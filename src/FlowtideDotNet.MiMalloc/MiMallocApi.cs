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

using System.Runtime.CompilerServices;

namespace FlowtideDotNet.MiMalloc
{
    /// <summary>
    /// Public entry points of the fully managed mimalloc port (v3.4.4).
    /// Names, argument order and semantics match the native mimalloc C API
    /// (<c>mimalloc.h</c>), so this class is a drop-in replacement for a
    /// P/Invoke binding. All functions are thread-safe; thread-local heaps are
    /// created on first use per thread and cleaned up automatically when a
    /// thread exits (pages with live blocks are abandoned and reclaimed by
    /// other threads, exactly as in native mimalloc).
    /// </summary>
    public static unsafe partial class MiMalloc
    {
        /// <summary>
        /// The mimalloc version this port is based on, encoded as in <c>mimalloc.h</c>:
        /// major + 2 digits minor + 2 digits patch. v3.4.4 is <c>30404</c>.
        /// </summary>
        public static int mi_version() => Mi.mi_version();

        // ------------------------------------------------------
        // Basic allocation
        // ------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_malloc(nuint size) => Mi.mi_malloc(size);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_zalloc(nuint size) => Mi.mi_zalloc(size);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_calloc(nuint count, nuint size) => Mi.mi_calloc(count, size);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_mallocn(nuint count, nuint size) => Mi.mi_mallocn(count, size);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_realloc(void* p, nuint newsize) => Mi.mi_realloc(p, newsize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_reallocn(void* p, nuint count, nuint size) => Mi.mi_reallocn(p, count, size);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_reallocf(void* p, nuint newsize) => Mi.mi_reallocf(p, newsize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_rezalloc(void* p, nuint newsize) => Mi.mi_rezalloc(p, newsize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_recalloc(void* p, nuint count, nuint size) => Mi.mi_recalloc(p, count, size);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_expand(void* p, nuint newsize) => Mi.mi_expand(p, newsize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void mi_free(void* p) => Mi.mi_free(p);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void mi_free_size(void* p, nuint size) => Mi.mi_free_size(p, size);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_malloc_small(nuint size) => Mi.mi_malloc_small(size);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_zalloc_small(nuint size) => Mi.mi_zalloc_small(size);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte* mi_strdup(byte* s) => Mi.mi_strdup(s);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte* mi_strndup(byte* s, nuint n) => Mi.mi_strndup(s, n);

        // ------------------------------------------------------
        // Size queries
        // ------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_usable_size(void* p) => Mi.mi_usable_size(p);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_good_size(nuint size) => Mi.mi_good_size(size);

        // ------------------------------------------------------
        // Aligned allocation
        // (note: `alignment` follows `size` in the mi_*_aligned functions,
        //  while `mi_aligned_alloc` uses the C11 posix argument order)
        // ------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_malloc_aligned(nuint size, nuint alignment) => Mi.mi_malloc_aligned(size, alignment);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_malloc_aligned_at(nuint size, nuint alignment, nuint offset) => Mi.mi_malloc_aligned_at(size, alignment, offset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_zalloc_aligned(nuint size, nuint alignment) => Mi.mi_zalloc_aligned(size, alignment);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_zalloc_aligned_at(nuint size, nuint alignment, nuint offset) => Mi.mi_zalloc_aligned_at(size, alignment, offset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_calloc_aligned(nuint count, nuint size, nuint alignment) => Mi.mi_calloc_aligned(count, size, alignment);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_realloc_aligned(void* p, nuint newsize, nuint alignment) => Mi.mi_realloc_aligned(p, newsize, alignment);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_realloc_aligned_at(void* p, nuint newsize, nuint alignment, nuint offset) => Mi.mi_realloc_aligned_at(p, newsize, alignment, offset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_rezalloc_aligned(void* p, nuint newsize, nuint alignment) => Mi.mi_rezalloc_aligned(p, newsize, alignment);

        /// <summary>C11 <c>aligned_alloc</c> (posix argument order: alignment first).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void* mi_aligned_alloc(nuint alignment, nuint size) => Mi.mi_malloc_aligned(size, alignment);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void mi_free_aligned(void* p, nuint alignment) => Mi.mi_free_aligned(p, alignment);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void mi_free_size_aligned(void* p, nuint size, nuint alignment) => Mi.mi_free_size_aligned(p, size, alignment);

        // ------------------------------------------------------
        // Collection and thread lifecycle
        // ------------------------------------------------------

        /// <summary>Eagerly free memory; with <paramref name="force"/> aggressively return memory to the OS.</summary>
        public static void mi_collect(bool force) => Mi.mi_collect(force);

        /// <summary>
        /// Initialize the allocator on the current thread. Optional: this is done
        /// automatically on the first allocation from a thread.
        /// </summary>
        public static void mi_thread_init() => Mi.mi_thread_init();

        /// <summary>
        /// Mark the current thread as belonging to a thread pool, which makes it shed pages
        /// aggressively rather than retain them for reuse (it stops retaining full pages and
        /// declines cross-thread page reclaim).
        /// <para>
        /// Only appropriate for threads that park for long periods while holding allocations.
        /// Do NOT call this for ordinary .NET ThreadPool / TPL worker threads that carry the
        /// application's main work: shedding pages there leaves mostly-full pages abandoned and
        /// pins their memory.
        /// </para>
        /// </summary>
        public static void mi_thread_set_in_threadpool() => Mi.mi_thread_set_in_threadpool();

        /// <summary>
        /// Release the current thread's heaps (abandoning pages that still contain
        /// live blocks). Optional: a GC-driven sentinel runs this automatically after
        /// a thread exits; call it explicitly for deterministic cleanup.
        /// </summary>
        public static void mi_thread_done() => Mi.mi_thread_done();
    }
}
