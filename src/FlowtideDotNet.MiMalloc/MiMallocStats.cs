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

namespace FlowtideDotNet.MiMalloc
{
    /// <summary>
    /// A snapshot of the allocator's memory usage. Obtained from <see cref="MiMalloc.GetStats"/>.
    /// All byte values are bytes.
    /// </summary>
    public readonly struct MiMallocStats
    {
        /// <summary>Address space currently reserved from the OS (virtual, not necessarily backed).</summary>
        public long ReservedBytes { get; init; }
        /// <summary>Peak reserved bytes.</summary>
        public long ReservedPeakBytes { get; init; }

        /// <summary>
        /// Bytes currently committed (backed by physical memory or page file). This is the number
        /// that tracks the process working set; <see cref="ReservedBytes"/> is mostly address space.
        /// </summary>
        public long CommittedBytes { get; init; }
        /// <summary>Peak committed bytes.</summary>
        public long CommittedPeakBytes { get; init; }

        /// <summary>Cumulative bytes purged (decommitted/reset) back to the OS since process start.</summary>
        public long PurgedBytesTotal { get; init; }

        /// <summary>Number of mimalloc pages currently in use (across all merged heaps).</summary>
        public long Pages { get; init; }
        /// <summary>Pages currently abandoned (owned by no thread, awaiting reclaim).</summary>
        public long PagesAbandoned { get; init; }

        /// <summary>
        /// Live allocated bytes in normal (non-huge) blocks, as of the last stats merge.
        /// See the remarks on <see cref="MiMalloc.GetStats"/> about staleness.
        /// </summary>
        public long LiveNormalBytes { get; init; }
        /// <summary>Live allocated bytes in huge blocks, as of the last stats merge.</summary>
        public long LiveHugeBytes { get; init; }

        /// <summary>Number of arenas created.</summary>
        public long ArenaCount { get; init; }
        /// <summary>Threads currently registered with the allocator.</summary>
        public long Threads { get; init; }
        /// <summary>Heaps currently alive.</summary>
        public long Heaps { get; init; }
        /// <summary>Thread-local heaps (theaps) currently alive.</summary>
        public long Theaps { get; init; }

        /// <summary>OS allocation calls (mmap / VirtualAlloc) made.</summary>
        public long OsAllocCalls { get; init; }
        /// <summary>OS commit calls made.</summary>
        public long CommitCalls { get; init; }
        /// <summary>OS purge (decommit/reset) calls made.</summary>
        public long PurgeCalls { get; init; }

        /// <summary>
        /// True when the live-allocation figures were folded up from the thread-local heaps before
        /// this snapshot was taken. When false, <see cref="LiveNormalBytes"/>,
        /// <see cref="LiveHugeBytes"/> and <see cref="Pages"/> only contain whatever threads
        /// happened to have merged already, and are typically near zero — they do NOT mean
        /// "nothing is allocated".
        /// </summary>
        public bool LiveStatsMerged { get; init; }

        /// <summary>
        /// Committed bytes not currently handed out as live blocks: free blocks inside pages plus
        /// committed-but-unused page space. A large and growing value relative to
        /// <see cref="CommittedBytes"/> indicates fragmentation or retention.
        /// <para>
        /// Null when <see cref="LiveStatsMerged"/> is false, because it cannot be computed from
        /// unmerged live figures. For an exact value that never depends on merging, use
        /// <see cref="MiMalloc.GetHeapUsage"/>.
        /// </para>
        /// </summary>
        public long? RetainedBytes
            => LiveStatsMerged ? CommittedBytes - LiveNormalBytes - LiveHugeBytes : null;

        public override string ToString()
        {
            static string MB(long b) => $"{b / (1024.0 * 1024.0):N1} MiB";
            string live = LiveStatsMerged
                ? $"live {MB(LiveNormalBytes + LiveHugeBytes)}, retained {MB(RetainedBytes!.Value)}"
                : "live/retained unknown (thread stats not merged)";
            return
                $"mimalloc: reserved {MB(ReservedBytes)} (peak {MB(ReservedPeakBytes)}), " +
                $"committed {MB(CommittedBytes)} (peak {MB(CommittedPeakBytes)}), " +
                $"{live}, purged total {MB(PurgedBytesTotal)}, " +
                $"pages {Pages} ({PagesAbandoned} abandoned), arenas {ArenaCount}, " +
                $"threads {Threads}, heaps {Heaps}, theaps {Theaps}, " +
                $"os-calls: alloc {OsAllocCalls}, commit {CommitCalls}, purge {PurgeCalls}";
        }
    }

    public static unsafe partial class MiMalloc
    {
        /// <summary>
        /// Take a snapshot of allocator memory usage.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The OS-level figures (<see cref="MiMallocStats.ReservedBytes"/>,
        /// <see cref="MiMallocStats.CommittedBytes"/>, <see cref="MiMallocStats.PurgedBytesTotal"/>
        /// and the call counts) are maintained with atomics on process-global state, so they are
        /// exact and current at the moment of the call. These are the numbers that answer
        /// "how much memory has mimalloc taken from the OS".
        /// </para>
        /// <para>
        /// The live-allocation figures (<see cref="MiMallocStats.LiveNormalBytes"/>,
        /// <see cref="MiMallocStats.LiveHugeBytes"/>, <see cref="MiMallocStats.Pages"/>) are
        /// accumulated per thread-local heap and only folded into the global totals when a thread
        /// collects or exits, so they lag. Pass <paramref name="mergeThreadStats"/> to fold in the
        /// CALLING thread's numbers first; other threads' contributions still lag until they
        /// collect. Use <see cref="MiMalloc.mi_collect"/> on each participating thread for an
        /// exact figure.
        /// </para>
        /// </remarks>
        /// <param name="mergeThreadStats">
        /// Merge the calling thread's pending statistics before reading (calls <c>mi_collect(false)</c>).
        /// </param>
        public static MiMallocStats GetStats(bool mergeThreadStats = false)
        {
            if (mergeThreadStats)
            {
                // folds this thread's theap stats up into its heap, and the heap's into the subproc
                Mi.mi_collect(false);
                Mi.mi_heap_stats_merge_to_subproc(null);
            }

            mi_subproc_t* subproc = Mi._mi_subproc_main();
            mi_stats_t* s = &subproc->stats;
            return new MiMallocStats
            {
                ReservedBytes = s->reserved.current,
                ReservedPeakBytes = s->reserved.peak,
                CommittedBytes = s->committed.current,
                CommittedPeakBytes = s->committed.peak,
                PurgedBytesTotal = s->purged.total,
                Pages = s->pages.current,
                PagesAbandoned = s->pages_abandoned.current,
                LiveNormalBytes = s->malloc_normal.current,
                LiveHugeBytes = s->malloc_huge.current,
                ArenaCount = s->arena_count.total,
                Threads = s->threads.current,
                Heaps = s->heaps.current,
                Theaps = s->theaps.current,
                OsAllocCalls = s->mmap_calls.total,
                CommitCalls = s->commit_calls.total,
                PurgeCalls = s->purge_calls.total,
                LiveStatsMerged = mergeThreadStats,
            };
        }
    }
}
