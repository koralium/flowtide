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
// Port of mimalloc v3.4.4 `src/options.c` (+ the mi_option_t enum from `mimalloc.h`).
// Original: Copyright (c) 2018-2026 Microsoft Research, Daan Leijen (MIT license).
//
// Differences from C (documented in README.md):
//  - The delayed-output buffer machinery is dropped: .NET can write to stderr at any
//    time and the port never runs before the runtime is initialized. mi_register_output
//    still works (defaults to stderr).
//  - printf-style formatting is replaced by interpolated strings at call sites.
//  - Option state is ordinary managed static state (like the C static table); an
//    initializing data race is benign as in C (all racers compute the same value).

using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FlowtideDotNet.MiMalloc
{
    // Options enum from mimalloc.h -- the numeric order defines the table index and must
    // match the C enum exactly.
    internal enum mi_option_t : int
    {
        // stable options
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
        mi_option_reserve_os_memory,          // reserve specified amount of OS memory in an arena at startup (in KiB)
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
        mi_option_deprecated_max_segment_reclaim,
        mi_option_destroy_on_exit,            // if set, release all memory on exit; sometimes used for dynamic unloading but can be unsafe
        mi_option_arena_reserve,              // initial memory size for arena reservation (= 1 GiB on 64-bit; in KiB)
        mi_option_arena_purge_mult,           // multiplier for `purge_delay` for the purging delay for arenas (=10)
        mi_option_deprecated_purge_extend_delay,
        mi_option_disallow_arena_alloc,       // 1 = do not use arena's for allocation (except if using specific arena id's)
        mi_option_retry_on_oom,               // retry on out-of-memory for N milli seconds (=400), set to 0 to disable retries. (only on windows)
        mi_option_deprecated_visit_abandoned,
        mi_option_guarded_min,                // only used when building with MI_GUARDED
        mi_option_guarded_max,                // only used when building with MI_GUARDED
        mi_option_guarded_precise,
        mi_option_guarded_sample_rate,
        mi_option_guarded_sample_seed,
        mi_option_generic_collect,            // collect theaps every N (=10000) generic allocation calls
        mi_option_page_reclaim_on_free,       // reclaim abandoned pages on a free (=0). -1 disallow always, 0 allows if the page originated from the current theap, 1 allow always
        mi_option_page_full_retain,           // retain N full (small) pages per size class (=2)
        mi_option_page_max_candidates,        // max candidate pages to consider for allocation (=4)
        mi_option_max_vabits,                 // max user space virtual address bits to consider (=48)
        mi_option_pagemap_commit,             // commit the full pagemap (to always catch invalid pointer uses) (=0)
        mi_option_page_commit_on_demand,      // commit page memory on-demand
        mi_option_page_max_reclaim,           // don't reclaim pages of the same originating theap if we already own N pages (in that size class) (=-1 (unlimited))
        mi_option_page_cross_thread_max_reclaim, // don't reclaim pages across threads if we already own N pages (in that size class) (=32)
        mi_option_allow_thp,                  // allow transparent huge pages? (=1). Set to 0 to disable THP for the process.
        mi_option_minimal_purge_size,         // set minimal purge size (in KiB) (=0)
        mi_option_arena_max_object_size,      // set maximal object size that can be allocated in an arena (in KiB) (=2GiB on 64-bit)
        mi_option_arena_is_numa_local,        // experimental
        _mi_option_last,
        // legacy option names
        mi_option_large_os_pages = mi_option_allow_large_os_pages,
        mi_option_eager_region_commit = mi_option_arena_eager_commit,
        mi_option_reset_decommits = mi_option_purge_decommits,
        mi_option_reset_delay = mi_option_purge_delay,
        mi_option_limit_os_alloc = mi_option_disallow_os_alloc
    }

    internal enum mi_option_init_t : int
    {
        MI_OPTION_UNINIT,       // not yet initialized
        MI_OPTION_DEFAULTED,    // not found in the environment, use default value
        MI_OPTION_INITIALIZED   // found in environment or set explicitly
    }

    internal sealed class mi_option_desc_t
    {
        public long value;                 // the value
        public mi_option_init_t init;      // is it initialized yet? (from the environment)
        public mi_option_t option;         // for debugging: the option index should match the option
        public string name = "";           // option name without `mimalloc_` prefix
        public string? legacy_name;        // potential legacy option name

        public mi_option_desc_t(long value, mi_option_t option, string name, string? legacyName = null)
        {
            this.value = value;
            this.init = mi_option_init_t.MI_OPTION_UNINIT;
            this.option = option;
            this.name = name;
            this.legacy_name = legacyName;
        }
    }

    internal delegate void mi_error_fun(int err, object? arg);
    internal delegate void mi_output_fun(string msg, object? arg);

    [InterpolatedStringHandler]
    internal ref struct MiTraceMessageHandler
    {
        private DefaultInterpolatedStringHandler _inner;
        public readonly bool Enabled;

        public MiTraceMessageHandler(int literalLength, int formattedCount, out bool shouldAppend)
        {
            Enabled = shouldAppend = Mi.mi_trace_message_enabled();
            _inner = shouldAppend ? new DefaultInterpolatedStringHandler(literalLength, formattedCount) : default;
        }

        public void AppendLiteral(string value) => _inner.AppendLiteral(value);
        public void AppendFormatted<T>(T value) => _inner.AppendFormatted(value);
        public void AppendFormatted<T>(T value, string? format) => _inner.AppendFormatted(value, format);
        public string ToStringAndClear() => _inner.ToStringAndClear();
    }

    [InterpolatedStringHandler]
    internal ref struct MiVerboseMessageHandler
    {
        private DefaultInterpolatedStringHandler _inner;
        public readonly bool Enabled;

        public MiVerboseMessageHandler(int literalLength, int formattedCount, out bool shouldAppend)
        {
            Enabled = shouldAppend = Mi.mi_verbose_message_enabled();
            _inner = shouldAppend ? new DefaultInterpolatedStringHandler(literalLength, formattedCount) : default;
        }

        public void AppendLiteral(string value) => _inner.AppendLiteral(value);
        public void AppendFormatted<T>(T value) => _inner.AppendFormatted(value);
        public void AppendFormatted<T>(T value, string? format) => _inner.AppendFormatted(value, format);
        public string ToStringAndClear() => _inner.ToStringAndClear();
    }

    [InterpolatedStringHandler]
    internal ref struct MiWarningMessageHandler
    {
        private DefaultInterpolatedStringHandler _inner;
        public readonly bool Enabled;

        public MiWarningMessageHandler(int literalLength, int formattedCount, out bool shouldAppend)
        {
            Enabled = shouldAppend = Mi.mi_warning_message_enabled();
            _inner = shouldAppend ? new DefaultInterpolatedStringHandler(literalLength, formattedCount) : default;
        }

        public void AppendLiteral(string value) => _inner.AppendLiteral(value);
        public void AppendFormatted<T>(T value) => _inner.AppendFormatted(value);
        public void AppendFormatted<T>(T value, string? format) => _inner.AppendFormatted(value, format);
        public string ToStringAndClear() => _inner.ToStringAndClear();
    }

    [InterpolatedStringHandler]
    internal ref struct MiErrorMessageHandler
    {
        private DefaultInterpolatedStringHandler _inner;
        public readonly bool Enabled;

        public MiErrorMessageHandler(int literalLength, int formattedCount, out bool shouldAppend)
        {
            Enabled = shouldAppend = Mi.mi_error_message_enabled();
            _inner = shouldAppend ? new DefaultInterpolatedStringHandler(literalLength, formattedCount) : default;
        }

        public void AppendLiteral(string value) => _inner.AppendLiteral(value);
        public void AppendFormatted<T>(T value) => _inner.AppendFormatted(value);
        public void AppendFormatted<T>(T value, string? format) => _inner.AppendFormatted(value, format);
        public string ToStringAndClear() => _inner.ToStringAndClear();
    }

    internal static unsafe partial class Mi
    {
        // C: mimalloc.h `MI_MALLOC_VERSION` -- major + 2 digits minor + 2 digits patch (v3.4.4)
        public const int MI_MALLOC_VERSION = 30404;

        private static long mi_max_error_count = 16;   // stop outputting errors after this (use < 0 for no limit)
        private static long mi_max_warning_count = 16; // stop outputting warnings after this (use < 0 for no limit)

        public static int mi_version() => MI_MALLOC_VERSION;

        // --------------------------------------------------------
        // Options
        // These can be accessed by multiple threads and may be
        // concurrently initialized, but an initializing data race
        // is ok since they resolve to the same value.
        // --------------------------------------------------------

        private const long MI_DEFAULT_VERBOSE = 0;
        private const long MI_DEFAULT_ARENA_EAGER_COMMIT = 2;
        private const long MI_DEFAULT_ARENA_RESERVE = 1024L * 1024L;   // in KiB: 1 GiB (64-bit)
        // (MI_SIZE_BITS * MI_ARENA_MAX_CHUNK_OBJ_SIZE) / MI_KiB = 2 GiB in KiB
        private const long MI_DEFAULT_ARENA_MAX_OBJECT_SIZE = (MI_SIZE_BITS * (long)MI_ARENA_MAX_CHUNK_OBJ_SIZE) / 1024;
        private const long MI_DEFAULT_DISALLOW_ARENA_ALLOC = 0;
        private const long MI_DEFAULT_ALLOW_LARGE_OS_PAGES = 0;
        private const long MI_DEFAULT_RESERVE_HUGE_OS_PAGES = 0;
        private const long MI_DEFAULT_RESERVE_OS_MEMORY = 0;
        private const long MI_DEFAULT_GUARDED_SAMPLE_RATE = 0;   // no MI_GUARDED
        private const long MI_DEFAULT_PAGE_MAX_RECLAIM = -1;     // unlimited
        private const long MI_DEFAULT_PAGE_CROSS_THREAD_MAX_RECLAIM = 32;
        private const long MI_DEFAULT_ALLOW_THP = 1;

#if MI_DEBUG
        private const long MI_DEFAULT_SHOW_ERRORS = 1;
#else
        private const long MI_DEFAULT_SHOW_ERRORS = 0;
#endif

        // Static options table; order MUST match mi_option_t.
        private static readonly mi_option_desc_t[] mi_options = new mi_option_desc_t[]
        {
            new(MI_DEFAULT_SHOW_ERRORS, mi_option_t.mi_option_show_errors, "show_errors"),
            new(0, mi_option_t.mi_option_show_stats, "show_stats"),
            new(MI_DEFAULT_VERBOSE, mi_option_t.mi_option_verbose, "verbose"),

            new(1, mi_option_t.mi_option_deprecated_eager_commit, "deprecated_eager_commit"),
            new(MI_DEFAULT_ARENA_EAGER_COMMIT, mi_option_t.mi_option_arena_eager_commit, "arena_eager_commit", "eager_region_commit"),
            new(1, mi_option_t.mi_option_purge_decommits, "purge_decommits", "reset_decommits"),
            new(MI_DEFAULT_ALLOW_LARGE_OS_PAGES, mi_option_t.mi_option_allow_large_os_pages, "allow_large_os_pages", "large_os_pages"),
            new(MI_DEFAULT_RESERVE_HUGE_OS_PAGES, mi_option_t.mi_option_reserve_huge_os_pages, "reserve_huge_os_pages"),
            new(-1, mi_option_t.mi_option_reserve_huge_os_pages_at, "reserve_huge_os_pages_at"),
            new(MI_DEFAULT_RESERVE_OS_MEMORY, mi_option_t.mi_option_reserve_os_memory, "reserve_os_memory"),
            new(0, mi_option_t.mi_option_deprecated_segment_cache, "deprecated_segment_cache"),
            new(0, mi_option_t.mi_option_deprecated_page_reset, "deprecated_page_reset"),
            new(0, mi_option_t.mi_option_deprecated_abandoned_page_purge, "deprecated_abandoned_page_purge"),
            new(0, mi_option_t.mi_option_deprecated_segment_reset, "deprecated_segment_reset"),
            new(1, mi_option_t.mi_option_deprecated_eager_commit_delay, "deprecated_eager_commit_delay"),
            new(1000, mi_option_t.mi_option_purge_delay, "purge_delay", "reset_delay"),
            new(0, mi_option_t.mi_option_use_numa_nodes, "use_numa_nodes"),
            new(0, mi_option_t.mi_option_disallow_os_alloc, "disallow_os_alloc", "limit_os_alloc"),
            new(100, mi_option_t.mi_option_os_tag, "os_tag"),
            new(32, mi_option_t.mi_option_max_errors, "max_errors"),
            new(32, mi_option_t.mi_option_max_warnings, "max_warnings"),
            new(10, mi_option_t.mi_option_deprecated_max_segment_reclaim, "deprecated_max_segment_reclaim"),
            new(0, mi_option_t.mi_option_destroy_on_exit, "destroy_on_exit"),
            new(MI_DEFAULT_ARENA_RESERVE, mi_option_t.mi_option_arena_reserve, "arena_reserve"),
            new(1, mi_option_t.mi_option_arena_purge_mult, "arena_purge_mult"),
            new(1, mi_option_t.mi_option_deprecated_purge_extend_delay, "deprecated_purge_extend_delay", "decommit_extend_delay"),
            new(MI_DEFAULT_DISALLOW_ARENA_ALLOC, mi_option_t.mi_option_disallow_arena_alloc, "disallow_arena_alloc"),
            new(400, mi_option_t.mi_option_retry_on_oom, "retry_on_oom"),
            new(1, mi_option_t.mi_option_deprecated_visit_abandoned, "deprecated_visit_abandoned"),
            new(0, mi_option_t.mi_option_guarded_min, "guarded_min"),
            new((long)MI_GiB, mi_option_t.mi_option_guarded_max, "guarded_max"),
            new(0, mi_option_t.mi_option_guarded_precise, "guarded_precise"),
            new(MI_DEFAULT_GUARDED_SAMPLE_RATE, mi_option_t.mi_option_guarded_sample_rate, "guarded_sample_rate"),
            new(0, mi_option_t.mi_option_guarded_sample_seed, "guarded_sample_seed"),
            new(10000, mi_option_t.mi_option_generic_collect, "generic_collect"),
            new(0, mi_option_t.mi_option_page_reclaim_on_free, "page_reclaim_on_free", "abandoned_reclaim_on_free"),
            new(2, mi_option_t.mi_option_page_full_retain, "page_full_retain"),
            new(4, mi_option_t.mi_option_page_max_candidates, "page_max_candidates"),
            new(0, mi_option_t.mi_option_max_vabits, "max_vabits"),
            // C default is 1 on macOS (mixed pointers when overriding malloc); we never
            // override the system allocator so the plain default 0 applies everywhere.
            new(0, mi_option_t.mi_option_pagemap_commit, "pagemap_commit"),
            new(0, mi_option_t.mi_option_page_commit_on_demand, "page_commit_on_demand"),
            new(MI_DEFAULT_PAGE_MAX_RECLAIM, mi_option_t.mi_option_page_max_reclaim, "page_max_reclaim"),
            new(MI_DEFAULT_PAGE_CROSS_THREAD_MAX_RECLAIM, mi_option_t.mi_option_page_cross_thread_max_reclaim, "page_cross_thread_max_reclaim"),
            new(MI_DEFAULT_ALLOW_THP, mi_option_t.mi_option_allow_thp, "allow_thp"),
            new(0, mi_option_t.mi_option_minimal_purge_size, "minimal_purge_size"),
            new(MI_DEFAULT_ARENA_MAX_OBJECT_SIZE, mi_option_t.mi_option_arena_max_object_size, "arena_max_object_size"),
            new(0, mi_option_t.mi_option_arena_is_numa_local, "arena_is_numa_local"),
        };

        private static bool mi_option_has_size_in_kib(mi_option_t option)
        {
            return option == mi_option_t.mi_option_reserve_os_memory
                || option == mi_option_t.mi_option_arena_reserve
                || option == mi_option_t.mi_option_minimal_purge_size
                || option == mi_option_t.mi_option_arena_max_object_size;
        }

        public static void _mi_options_init()
        {
            // called on process load
            for (int i = 0; i < (int)mi_option_t._mi_option_last; i++)
            {
                mi_option_get((mi_option_t)i); // initialize
            }
            mi_max_error_count = mi_option_get(mi_option_t.mi_option_max_errors);
            mi_max_warning_count = mi_option_get(mi_option_t.mi_option_max_warnings);
        }

        // C: options.c `mi_options_print_out`. (`_mi_fprintf(out,arg,..)` falls back to the
        // registered/default output when `out` is null.)
        public static void mi_options_print_out(mi_output_fun? @out, object? arg)
        {
            static void emit(mi_output_fun? o, object? a, string msg)
            {
                if (o != null) { o(msg, a); } else { mi_out(msg); }
            }

            // show version
            int vermajor = MI_MALLOC_VERSION / 10000;
            int verminor = (MI_MALLOC_VERSION % 10000) / 100;
            int verpatch = MI_MALLOC_VERSION % 100;
            emit(@out, arg, $"v{vermajor}.{verminor}.{verpatch} (managed port)\n");

            // show options
            for (int i = 0; i < (int)mi_option_t._mi_option_last; i++)
            {
                mi_option_t option = (mi_option_t)i;
                mi_option_get(option);   // possibly initialize
                mi_option_desc_t desc = mi_options[i];
                emit(@out, arg, $"option '{desc.name}': {desc.value} {(mi_option_has_size_in_kib(option) ? "KiB" : "")}\n");
            }

            // show build configuration
#if MI_DEBUG
            emit(@out, arg, "debug level : 2\n");
#else
            emit(@out, arg, "debug level : 0\n");
#endif
            emit(@out, arg, "secure level: 0\n");
            emit(@out, arg, "mem tracking: none\n");
        }

        // C: options.c `mi_options_print`
        public static void mi_options_print() => mi_options_print_out(null, null);

        // C: options.c `_mi_options_post_init`. (The `mi_add_stderr_output` half is a
        // documented deviation -- this port has no delayed output buffer, `mi_out`
        // writes to stderr from the start.)
        public static void _mi_options_post_init()
        {
            if (mi_option_is_enabled(mi_option_t.mi_option_verbose)) { mi_options_print(); }
        }

        public static long _mi_option_get_fast(mi_option_t option)
        {
            mi_assert(option >= 0 && option < mi_option_t._mi_option_last);
            mi_option_desc_t desc = mi_options[(int)option];
            mi_assert(desc.option == option);  // index should match the option
            return desc.value;
        }

        public static long mi_option_get(mi_option_t option)
        {
            mi_assert(option >= 0 && option < mi_option_t._mi_option_last);
            if (option < 0 || option >= mi_option_t._mi_option_last) return 0;
            mi_option_desc_t desc = mi_options[(int)option];
            mi_assert(desc.option == option);  // index should match the option
            if (desc.init == mi_option_init_t.MI_OPTION_UNINIT)
            {
                mi_option_init(desc);
            }
            return desc.value;
        }

        public static long mi_option_get_clamp(mi_option_t option, long min, long max)
        {
            long x = mi_option_get(option);
            return x < min ? min : (x > max ? max : x);
        }

        public static nuint mi_option_get_size(mi_option_t option)
        {
            long x = mi_option_get(option);
            nuint size = x < 0 ? 0 : (nuint)x;
            if (mi_option_has_size_in_kib(option))
            {
                if (mi_mul_overflow(size, MI_KiB, out size))
                {
                    size = unchecked((nuint)MI_MAX_ALLOC_SIZE);
                }
            }
            return size;
        }

        public static void mi_option_set(mi_option_t option, long value)
        {
            mi_assert(option >= 0 && option < mi_option_t._mi_option_last);
            if (option < 0 || option >= mi_option_t._mi_option_last) return;
            mi_option_desc_t desc = mi_options[(int)option];
            mi_assert(desc.option == option);  // index should match the option
            desc.value = value;
            desc.init = mi_option_init_t.MI_OPTION_INITIALIZED;
            // ensure min/max range; be careful to not recurse.
            if (desc.option == mi_option_t.mi_option_guarded_min && _mi_option_get_fast(mi_option_t.mi_option_guarded_max) < value)
            {
                mi_option_set(mi_option_t.mi_option_guarded_max, value);
            }
            else if (desc.option == mi_option_t.mi_option_guarded_max && _mi_option_get_fast(mi_option_t.mi_option_guarded_min) > value)
            {
                mi_option_set(mi_option_t.mi_option_guarded_min, value);
            }
        }

        public static void mi_option_set_default(mi_option_t option, long value)
        {
            mi_assert(option >= 0 && option < mi_option_t._mi_option_last);
            if (option < 0 || option >= mi_option_t._mi_option_last) return;
            mi_option_desc_t desc = mi_options[(int)option];
            if (desc.init != mi_option_init_t.MI_OPTION_INITIALIZED)
            {
                desc.value = value;
            }
        }

        public static bool mi_option_is_enabled(mi_option_t option) => mi_option_get(option) != 0;

        public static void mi_option_set_enabled(mi_option_t option, bool enable) => mi_option_set(option, enable ? 1 : 0);

        public static void mi_option_set_enabled_default(mi_option_t option, bool enable) => mi_option_set_default(option, enable ? 1 : 0);

        public static void mi_option_enable(mi_option_t option) => mi_option_set_enabled(option, true);

        public static void mi_option_disable(mi_option_t option) => mi_option_set_enabled(option, false);

        // --------------------------------------------------------
        // Initialize options by checking the environment
        // --------------------------------------------------------

        // C `_mi_getenv`: try the name as-is, then upper-cased.
        private static string? mi_getenv(string name)
        {
            string? s = Environment.GetEnvironmentVariable(name);
            if (s == null)
            {
                s = Environment.GetEnvironmentVariable(name.ToUpperInvariant());
            }
            return s;
        }

        private static void mi_option_init(mi_option_desc_t desc)
        {
            // Read option value from the environment
            string? s = mi_getenv("mimalloc_" + desc.name);
            if (s == null && desc.legacy_name != null)
            {
                s = mi_getenv("mimalloc_" + desc.legacy_name);
                if (s != null)
                {
                    _mi_warning_message($"environment option \"mimalloc_{desc.legacy_name}\" is deprecated -- use \"mimalloc_{desc.name}\" instead.");
                }
            }

            if (s != null)
            {
                // note: no Trim() -- C matches the raw (uppercased) value exactly for the
                // bool words, and strtol only skips LEADING whitespace for numbers
                string buf = s.ToUpperInvariant();
                if (buf.Length == 0 || buf == "1" || buf == "TRUE" || buf == "YES" || buf == "ON")
                {
                    desc.value = 1;
                    desc.init = mi_option_init_t.MI_OPTION_INITIALIZED;
                }
                else if (buf == "0" || buf == "FALSE" || buf == "NO" || buf == "OFF")
                {
                    desc.value = 0;
                    desc.init = mi_option_init_t.MI_OPTION_INITIALIZED;
                }
                else
                {
                    // parse a decimal number with optional size suffix (strtol semantics:
                    // skip leading whitespace, optional sign, digits)
                    int pos = 0;
                    while (pos < buf.Length && char.IsWhiteSpace(buf[pos])) pos++;
                    int signStart = pos;
                    if (pos < buf.Length && (buf[pos] == '-' || buf[pos] == '+')) pos++;
                    int digitsStart = pos;
                    while (pos < buf.Length && char.IsAsciiDigit(buf[pos])) pos++;
                    long value = 0;
                    bool ok;
                    if (pos == digitsStart)
                    {
                        // C `strtol` performs no conversion here: it returns 0, leaves errno == 0,
                        // and stores the ORIGINAL pointer in `end` (not the position past the
                        // whitespace/sign). So a bare size suffix such as "KB" is still accepted,
                        // with value 0.
                        ok = true;
                        value = 0;
                        pos = 0;
                    }
                    else
                    {
                        // a parse failure here is C's ERANGE (errno != 0)
                        ok = long.TryParse(buf.AsSpan(signStart, pos - signStart), NumberStyles.Integer,
                                           CultureInfo.InvariantCulture, out value);
                    }
                    if (ok && mi_option_has_size_in_kib(desc.option))
                    {
                        // this option is interpreted in KiB to prevent overflow of `long`
                        nuint size = value < 0 ? 0 : (nuint)value;
                        bool overflow = false;
                        if (pos < buf.Length && buf[pos] == 'K') { pos++; }
                        else if (pos < buf.Length && buf[pos] == 'M') { overflow = mi_mul_overflow(size, MI_KiB, out size); pos++; }
                        else if (pos < buf.Length && buf[pos] == 'G') { overflow = mi_mul_overflow(size, MI_MiB, out size); pos++; }
                        else if (pos < buf.Length && buf[pos] == 'T') { overflow = mi_mul_overflow(size, MI_GiB, out size); pos++; }
                        else { size = (size + MI_KiB - 1) / MI_KiB; }
                        if (pos + 1 < buf.Length && buf[pos] == 'I' && buf[pos + 1] == 'B') { pos += 2; }   // KIB, MIB, GIB, TIB
                        else if (pos < buf.Length && buf[pos] == 'B') { pos++; }                            // KB, MB, GB, TB
                        if (overflow || size > unchecked((nuint)(MI_MAX_ALLOC_SIZE / 1024))) { size = unchecked((nuint)(MI_MAX_ALLOC_SIZE / 1024)); }
                        value = size > long.MaxValue ? long.MaxValue : (long)size;
                    }
                    if (ok && pos == buf.Length)
                    {
                        mi_option_set(desc.option, value);
                    }
                    else
                    {
                        // set `init` first to avoid recursion through _mi_warning_message on mimalloc_verbose.
                        desc.init = mi_option_init_t.MI_OPTION_DEFAULTED;
                        if (desc.option == mi_option_t.mi_option_verbose && desc.value == 0)
                        {
                            // if the 'mimalloc_verbose' env var has a bogus value we'd never know
                            // (since the value defaults to 'off') so in that case briefly enable verbose
                            desc.value = 1;
                            _mi_warning_message($"environment option mimalloc_{desc.name} has an invalid value.");
                            desc.value = 0;
                        }
                        else
                        {
                            _mi_warning_message($"environment option mimalloc_{desc.name} has an invalid value.");
                        }
                    }
                }
                mi_assert_internal(desc.init != mi_option_init_t.MI_OPTION_UNINIT);
            }
            else
            {
                desc.init = mi_option_init_t.MI_OPTION_DEFAULTED;
            }
        }

        // --------------------------------------------------------
        // Output and messages
        // --------------------------------------------------------

        // C declares both as `_Atomic(void*)` and uses store_release / load_acquire; the
        // port must go through the volatile accessors for the same ordering. (As the C
        // comment notes, a handler must still be installed from a single thread: the
        // function and its argument are two separate stores, so a racing reader can pair
        // a new function with a stale argument -- that is the C's behaviour too.)
        private static mi_output_fun? mi_out_default;   // null -> stderr
        private static object? mi_out_arg;

        public static void mi_register_output(mi_output_fun? @out, object? arg)
        {
            Volatile.Write(ref mi_out_default, @out);
            Volatile.Write(ref mi_out_arg, arg);
        }

        // C: `mi_out_stderr` -> `_mi_prim_out_stderr` is noexcept -- `fputs` reports failure
        // through a return code and can never unwind. Every message site sits in the middle of
        // an allocator failure path that has already mutated shared state (claimed arena slices,
        // taken ownership of a page, ...), so an exception escaping here would skip the rollback
        // and permanently corrupt that state. Console.Error.Write throws IOException for a
        // broken/closed stderr (only EPIPE-style errors are swallowed by the runtime), and a
        // user-registered handler can throw anything: contain both.
        private static void mi_out(string msg)
        {
            try
            {
                mi_output_fun? @out = Volatile.Read(ref mi_out_default);
                if (@out != null)
                {
                    @out(msg, Volatile.Read(ref mi_out_arg));
                }
                else
                {
                    Console.Error.Write(msg);
                }
            }
            catch
            {
                // diagnostics must never take down the allocator (see above)
            }
        }

        private static long error_count;     // when >= max_error_count stop emitting errors
        private static long warning_count;   // when >= max_warning_count stop emitting warnings

        // C: `mi_vfprintf_thread` -- the thread id is only inserted for non-main threads
        // (and only when the prefix fits the 64-byte stack buffer it builds).
        private static void mi_message_with_thread_prefix(string prefix, string message)
        {
            if (prefix.Length <= 32 && !_mi_is_main_thread())
            {
                mi_out($"{prefix}thread 0x{_mi_thread_id():x}: {message}\n");
            }
            else
            {
                mi_out($"{prefix}{message}\n");
            }
        }

        internal static bool mi_trace_message_enabled()
            => mi_option_get(mi_option_t.mi_option_verbose) > 1;   // only with verbose level 2 or higher

        internal static bool mi_verbose_message_enabled()
            => mi_option_is_enabled(mi_option_t.mi_option_verbose);

        internal static bool mi_warning_message_enabled()
        {
            if (!mi_option_is_enabled(mi_option_t.mi_option_verbose))
            {
                if (!mi_option_is_enabled(mi_option_t.mi_option_show_errors)) return false;
                // C: fetch_add returns the PREVIOUS value, so max+1 messages are printed
                if (mi_max_warning_count >= 0 && Interlocked.Increment(ref warning_count) - 1 > mi_max_warning_count) return false;
            }
            return true;
        }

        internal static bool mi_error_message_enabled()
            => mi_option_is_enabled(mi_option_t.mi_option_verbose)
               || (mi_option_is_enabled(mi_option_t.mi_option_show_errors)
                   && (mi_max_error_count < 0 || Interlocked.Increment(ref error_count) - 1 <= mi_max_error_count));

        public static void _mi_trace_message(string message)
        {
            if (!mi_trace_message_enabled()) return;
            mi_message_with_thread_prefix("mimalloc: ", message);
        }

        public static void _mi_trace_message(ref MiTraceMessageHandler message)
        {
            if (!message.Enabled) return;
            mi_message_with_thread_prefix("mimalloc: ", message.ToStringAndClear());
        }

        public static void _mi_verbose_message(string message)
        {
            if (!mi_verbose_message_enabled()) return;
            mi_out($"mimalloc: {message}\n");
        }

        public static void _mi_verbose_message(ref MiVerboseMessageHandler message)
        {
            if (!message.Enabled) return;
            mi_out($"mimalloc: {message.ToStringAndClear()}\n");
        }

        public static void _mi_warning_message(string message)
        {
            if (!mi_warning_message_enabled()) return;
            mi_message_with_thread_prefix("mimalloc: warning: ", message);
        }

        public static void _mi_warning_message(ref MiWarningMessageHandler message)
        {
            if (!message.Enabled) return;
            mi_message_with_thread_prefix("mimalloc: warning: ", message.ToStringAndClear());
        }

        // --------------------------------------------------------
        // Errors
        // --------------------------------------------------------

        // C: `static mi_error_fun* volatile mi_error_handler` + `_Atomic(void*) mi_error_arg`
        private static mi_error_fun? mi_error_handler;
        private static object? mi_error_arg;

        private static void mi_error_default(int err)
        {
#if MI_DEBUG
            if (err == EFAULT)
            {
                // corrupted meta-data or double free: fail loudly in debug builds (C aborts)
                Debug.Fail("mimalloc: fatal internal error (EFAULT): corrupted meta-data or invalid free");
            }
#endif
        }

        public static void mi_register_error(mi_error_fun? fun, object? arg)
        {
            Volatile.Write(ref mi_error_handler, fun);  // can be null
            Volatile.Write(ref mi_error_arg, arg);
        }

        public static void _mi_error_message(int err, string message)
        {
            // show detailed error message
            if (mi_error_message_enabled()) { mi_message_with_thread_prefix("mimalloc: error: ", message); }
            mi_error_message_invoke_handler(err);
        }

        public static void _mi_error_message(int err, ref MiErrorMessageHandler message)
        {
            if (message.Enabled) { mi_message_with_thread_prefix("mimalloc: error: ", message.ToStringAndClear()); }
            mi_error_message_invoke_handler(err);
        }

        private static void mi_error_message_invoke_handler(int err)
        {
            // call the error handler which may abort (or return normally)
            // (snapshot into a local: C re-reads the volatile `mi_error_handler` for the call,
            //  which would fault if it were cleared between the test and the call)
            mi_error_fun? handler = Volatile.Read(ref mi_error_handler);
            if (handler != null)
            {
                try
                {
                    handler(err, Volatile.Read(ref mi_error_arg));
                }
                catch
                {
                    // ignored on purpose
                }
            }
            else
            {
                mi_error_default(err);
            }
        }
    }
}
