# mimalloc v3.4.4 → C# port (FlowtideDotNet.MiMalloc)

A faithful managed port of [mimalloc](https://github.com/microsoft/mimalloc) v3.4.4 so Flowtide
can use mimalloc's allocation strategy without shipping a native library.

- **C source of truth:** `C:\Sweco\Dev\OpenSource\mimalloc`, branch `mimalloc-v3.4.4`
  (tag `v3.4.4`, commit `1f06f694`). Always diff against this exact revision.
- **Consumer:** `src/FlowtideDotNet.Storage/Memory/FlowtideMemoryAllocation.cs` — the API surface
  Flowtide actually needs is small: `mi_aligned_alloc`, `mi_realloc_aligned`, `mi_free_aligned`,
  `mi_good_size`, `mi_collect`, plus the plain `mi_malloc`/`mi_zalloc`/`mi_free`/`mi_usable_size`.
- **Correctness is the top priority.** Thread-local heaps, lock-free cross-thread frees, and the
  atomic bitmaps must behave exactly like the C code. Performance second, features third.

## Fixed configuration (compile-time in C, baked-in here)

The C code has many `#if` variants. This port pins ONE configuration (the 64-bit release default):

| C define | Value | Notes |
|---|---|---|
| 64-bit only | yes | `MI_SIZE_SHIFT=3`, `MI_INTPTR_SHIFT=3`, `MI_SIZE_BITS=64`. Runtime-guard against 32-bit. |
| `MI_SECURE` | 0 | no guard pages / freelist encoding |
| `MI_DEBUG` | 0/2 | assertions become `mi_assert`/`mi_assert_internal` → `Debug.Assert` under `MI_DEBUG` symbol (Debug builds) |
| `MI_STAT` | 1 | full stat machinery is implemented (Stats.cs); `MI_STAT>0` blocks included (malloc_normal/malloc_huge), `MI_STAT>1` blocks (malloc_bins, requested) skipped |
| `MI_PADDING`, `MI_ENCODE_FREELIST`, `MI_GUARDED` | 0 | debug/security features, not correctness. `keys[2]` field kept out of structs. |
| `MI_ENABLE_LARGE_PAGES` | 1 | large pages (4 MiB) enabled — default |
| `MI_PAGE_MAP_FLAT` | 0 | two-level page map (x64 MAX_VABITS=47 > 40 → default) |
| `MI_PAGE_META_IS_SEPARATED` | 1 | page meta stored at arena start (default when page map not flat) |
| `MI_PAGE_META_ALIGNED_FREE_SMALL` | 0 | default |
| `MI_ARENA_SLICE_SHIFT` | 16 (64 KiB) | `13 + MI_SIZE_SHIFT` |
| `MI_BCHUNK_BITS_SHIFT` | 9 (512 bits) | `6 + MI_SIZE_SHIFT` |
| `MI_MAX_VABITS` / `MI_MIN_VABITS` | 47 / 43 | x64 values; arm64 uses 48/43 — use 48/43 to cover both (page-map sizing only) |
| `MI_BIN_HUGE` | 73 | 73 size bins + full bin |
| huge OS pages (1 GiB), `mremap`, NUMA | out of scope | `mi_option` hooks exist but no-op |
| sub-processes | main subproc only | struct is ported (it is the root object) but `mi_subproc_new` can come later |
| `MI_TRACK_*` (valgrind/asan/ETW) | 0 | no-ops |

## C → C# mapping conventions

- **Project:** `FlowtideDotNet.MiMalloc`, namespace `FlowtideDotNet.MiMalloc`. One C# file per C
  source file, same name (`bitmap.c` → `Bitmap.cs`, `arena.c` → `Arena.cs`, ...). Static unsafe
  classes; functions keep their C names (`mi_page_free_collect`) so code reviews can diff
  side-by-side against the C.
- **Structs:** unsafe `struct` with `LayoutKind.Sequential`, C names kept (`mi_page_t`).
  All metadata lives in *native* memory (inside arenas / meta pages), exactly like C — the GC never
  sees it. No managed references inside any mimalloc struct.
- **Fixed arrays in structs:**
  - arrays of primitives → C# `fixed` buffers (`fixed ulong x[N]`);
  - arrays of pointers → `fixed ulong` + cast on access (fixed buffers don't take pointer types);
  - arrays of structs → `[InlineArray(N)]` wrapper structs (net8.0+).
- **Atomics:** `Atomic.cs` provides `mi_atomic_*` named exactly like the C macros
  (`mi_atomic_load_acquire`, `mi_atomic_cas_weak_release`, ...). Implementation rules:
  - all RMW (cas/exchange/add/sub/and/or) → `Interlocked.*` (full fence — always ≥ the C ordering, so correct);
  - `load_acquire`/`store_release` → `Volatile.Read`/`Volatile.Write`;
  - `load_relaxed`/`store_relaxed` → plain access of an aligned word (atomic on all .NET platforms);
    implemented as `Volatile`-free direct read via ref.
  - C `weak` CAS → same as strong CAS in C# (spurious-failure loops still correct).
  - `_Atomic(T)` struct fields are declared as the plain type; ALL access must go through the
    `mi_atomic_*` helpers (same discipline as C).
- **`mi_lock_t`** (used only on slow paths: theaps list, arena reserve, os-abandoned list) →
  small unmanaged spinlock struct (int + `Interlocked` + `SpinWait`). Must stay unmanaged since it
  is embedded in native-memory structs.
- **Bit intrinsics (`bits.h`):** `System.Numerics.BitOperations` (`PopCount`,
  `TrailingZeroCount`, `LeadingZeroCount`, `RotateLeft/Right`) — JIT hardware intrinsics on
  x64/arm64. `mi_ctz(0)==64`, `mi_clz(0)==64` semantics are matched by BitOperations already.
- **Thread-locals:** all per-thread native state lives in ONE meta-allocated
  `mi_thread_ctx_t` behind a single `[ThreadStatic]` pointer (Init.cs): `theap_default` /
  `theap_cached` (null until set — `MI_THEAP_INITASNULL` semantics like the C Windows model),
  the `threadlocal.c` slots array, the fast slot, and the thread id. Thread-exit
  (`mi_thread_done`) has no reliable hook in .NET → a per-thread sentinel class in a
  `[ThreadStatic]` field whose finalizer runs `_mi_thread_done` after the thread dies,
  temporarily ADOPTING the dead thread's id (`_mi_prim_thread_id_swap_raw`) and ctx pointer
  so the C same-thread cleanup logic (incl. the `tld->thread_id` guard) runs unchanged on
  the finalizer thread; the ctx itself is meta-freed at the end. Explicit `mi_thread_done`
  on a live thread keeps the ctx armed (the thread may allocate again, as in C).
- **OS layer (`os.c` + `src/prim`):** P/Invoke straight to the OS: `VirtualAlloc/VirtualFree/`
  `VirtualProtect` (Windows), `mmap/munmap/madvise/mprotect` (Linux/macOS via libc). This keeps
  reserve/commit/decommit/purge semantics (`NativeMemory` can't do reserve-only). No bundled
  native binaries — only OS syscalls.
- **`memset`/`memcpy`** → `NativeMemory.Clear/Fill`, `Unsafe.CopyBlockUnaligned`, `Buffer.MemoryCopy`.
- **Options (`options.c`):** read the same `MIMALLOC_*` environment variables via
  `Environment.GetEnvironmentVariable` (Flowtide already sets `MIMALLOC_PURGE_DECOMMITS` etc.).
- **Asserts:** `mi_assert_internal`/`mi_assert_expensive` → `[Conditional("MI_DEBUG")]` helpers
  calling `Debug.Assert`. The `MI_DEBUG` symbol is defined for Debug builds in the csproj.
- **Error handling:** `_mi_error_message` → trace + return null, as in C. Public Flowtide entry
  points throw on OOM (as `FlowtideMemoryAllocation` does today).

## Porting order & status

Dependency-ordered. Update the checkboxes as work lands; also mirrored in the session task list.

- [x] 0. Scaffold: csproj (net8.0;net10.0, unsafe), tests project, slnx wiring, this doc
- [x] 1. Foundation — `Bits.cs` (bits.h), `Atomic.cs` (atomic.h), `Types.cs` (types.h +
      mimalloc.h constants), `Stats.cs` (mimalloc-stats.h structs), `Internal.cs` (pure
      helpers), `Libc.cs` (memzero/memcpy + messages), `Asserts.cs`, `Prim.cs`
      (thread id). 31 unit tests green on net8.0 + net10.0 (bits semantics, C11 CAS
      semantics, lock-free stack stress, lock mutual exclusion, once, thread-id contract,
      constants vs. C values, memid union, InlineArray indexing).
- [x] 2. `Os.cs` — os.c + prim layer (`PrimOs.cs`: VirtualAlloc/VirtualAlloc2/VirtualFree on
      Windows, mmap/madvise/mprotect on Linux/macOS with per-OS constants; commit, decommit,
      purge, reset, aligned alloc, OOM retry loop, interior-pointer free recovery).
      `Options.cs` (full options.c port: table, env parsing with strtol semantics, message
      gating) and the stats update functions in `Stats.cs` came along as dependencies.
      Real memory roundtrip tests green. Adversarially verified vs the C by a 6-agent
      find + 7-agent verify workflow: os.c comparer found ZERO issues; 4 minor
      diagnostics-path findings confirmed and fixed (MI_DEBUG-only overflow message,
      max-count off-by-one, bogus-verbose special case, no-trim strtol parsing).
- [x] 3. `Bitmap.cs` — bitmap.h + bitmap.c (atomic bfield/bchunk/bitmap/bbitmap + chunkmap;
      THE lock-free core) + layout/functional/stress tests incl. concurrent claim/release
      with shadow-array double-claim detection. Portable (non-SIMD) paths only
      (C default MI_OPT_SIMD=0 too). Variable-length `chunks[]` accessed via pointer
      helpers (mi_bitmap_chunk/mi_bbitmap_chunk), never the bounds-checked InlineArray
      indexer. The C iterate macros became the `mi_bfield_cycle_iter` struct; C callbacks
      became `delegate*`s; C gotos kept as C# gotos.
- [x] 4. `ArenaMeta.cs` (arena-meta.c; TEMPORARY arena stubs fall back to OS allocation
      until Arena.cs lands -- see the marked section at the bottom of the file),
      `PageMap.cs` (two-level page map + internal.h page accessors in Internal.cs +
      minimal `Init.cs` with `_mi_subproc_main`), `Random.cs` (chacha20, verified
      against the RFC 8439 vectors). Note: pointer-to-pointer atomic cells (submap
      entries) go through the nuint atomics -- C# generics reject `T*` type args.
- [x] 5. `Arena.cs` — the MEMORY half of arena.c: slice alloc (`mi_arena_try_alloc_at`,
      `_mi_arenas_alloc_aligned`), arena reserve/creation (`mi_reserve_os_memory*`,
      `mi_manage_os_memory*`, `mi_arena_initialize` with bitmap carving), free with
      delayed purging (schedule/try_purge incl. the `mi_atomic_guard` pattern), and
      the `mi_forall_arenas` iteration. Adversarially verified: zero confirmed
      findings. The PAGE half of arena.c (page alloc/free/abandon/unabandon +
      heap-visiting) moves to step 6 with page.c — one concurrency unit.
      Init.cs now wires the main heap (subproc->heap_main) so arena allocation works;
      ArenaMeta.cs stubs removed (meta allocator is arena-backed now).
- [x] 6. `PageQueue.cs` (page-queue.c: mi_bin with the MI_ALIGN2W branch, queue ops,
      pages_free_direct maintenance), `Page.cs` (page.c: thread-free collect, page
      init/extend with commit-on-demand, retire, full-queue, find-free candidate
      search; `_mi_malloc_generic`/`mi_find_page` deferred to step 8 with alloc.c),
      `ArenaPages.cs` (the page half of arena.c: fresh/abandoned page alloc, free,
      abandon/unabandon with the ownership protocol), the page-flag/ownership
      inlines + theap accessors into Internal.cs, and the empty statics
      (`_mi_page_empty`, `_mi_theap_empty(_wrong)`, bin-size table) + lazy
      `mi_process_init` (options → OS → subproc/heap_main → page map) into Init.cs.
      Adversarially verified (3 finders + 2 refuters): ArenaPages comparer found
      ZERO issues; one confirmed minor fixed (deferred-free handler/arg needed the
      C's Volatile ordering). Remaining stubs: `mi_arena_pages_alloc` (non-main
      heaps; step 8/9) and `_mi_page_associated_theap_peek` (threadlocal; step 7→8).
- [x] 7. `Heap.cs`, `Theap.cs`, `ThreadLocalMi.cs`, `Init.cs` — heaps, theaps, dynamic TLS,
      process/thread init, thread-exit abandonment. Design (deviations documented in
      the file headers): no static main theap/tld — EVERY thread meta-allocates its
      tld+theap (`.NET` needs no allocation-free bootstrap); all per-thread state in
      one native `mi_thread_ctx_t` behind a single `[ThreadStatic]` pointer
      (theap_default/cached with `MI_THEAP_INITASNULL` semantics, dynamic-TLS slots
      array, fast slot, thread id); thread exit via a finalizable sentinel whose
      finalizer runs `_mi_thread_done` on the finalizer thread while ADOPTING the
      dead thread's id + ctx (swap/restore), so C's same-thread checks and cleanup
      (abandon-then-free of theaps, tld free) run unchanged; threadlocal.c slots
      array uses the meta allocator (memid stored in a header field) instead of
      `mi_rezalloc` until task #9. `_mi_page_associated_theap_peek` stub replaced
      with the real prim-tls implementation (Heap.cs); real `_mi_subproc()`;
      `mi_heap_new/delete/destroy` + `mi_subproc_new` deferred to task #9.
- [x] 8. `Alloc.cs`, `Free.cs`, `AllocAligned.cs` — malloc/zalloc/calloc/realloc fast paths,
      multithreaded free with ownership claim, aligned alloc, usable_size/good_size, collect.
      Also: `mi_find_page` + `_mi_malloc_generic` (Page.cs), the heap lifecycle
      (`mi_heap_new/delete/destroy` in Heap.cs), heap-wide visiting + page
      move/destroy + real `mi_arena_pages_alloc` (ArenaPages.cs), `_mi_free_subproc_safe`,
      and threadlocal.c switched to real `mi_rezalloc`/`mi_free` (task-#8 meta-alloc
      deviation removed). Not ported: `mi_realpath`, the C++ `mi_new_*` family,
      `mi_subproc_new/destroy` (single-subproc pinned). One latent task-#7 issue
      surfaced by 8-byte blocks (unreachable in C debug builds where MI_PADDING>=1):
      two over-strong asserts in `mi_arenas_page_alloc_fresh` relaxed to the
      reachable invariant; the computation matches C release byte-for-byte.
- [x] 9. Public API class `MiMalloc` (MiMallocApi.cs: the full mi_* surface with native
      names/signatures incl. posix-order `mi_aligned_alloc`), integration into
      `FlowtideMemoryAllocation` (managed port is the DEFAULT; `FLOWTIDE_NATIVE_MIMALLOC=1`
      restores the native-library path), allocator benchmarks
      (tests/FlowtideDotNet.Benchmarks/MiMallocAllocatorBenchmark.cs: managed vs native
      vs NativeMemory -- alloc/free, mixed churn, realloc-grow, 4-thread MT).
      Storage test suite parity: all 352 tests run under the managed allocator on
      net8.0 Debug AND net10.0 Release with results IDENTICAL to the native allocator
      (1 pre-existing `TestCommit` golden-string failure on all configs; on net10.0
      six more pre-existing golden failures caused by zlib-ng compressed-URL diffs,
      identical under native). One pre-existing flaky test fixed
      (`TestProactiveEvictionLoopEvictsFilesWhenThresholdReached`: its single
      FakeTimeProvider advance raced the eviction loop's async delay re-arm and hung
      the suite; Debug-mode MI_DEBUG slowness made the race reliable).
      C# gotcha hit during integration: the new `FlowtideDotNet.MiMalloc` NAMESPACE
      shadows the simple type name `MiMalloc` anywhere under `FlowtideDotNet.*`
      (namespace members beat file-level usings), so consumers alias BOTH classes
      INSIDE their namespace block.
      Benchmarks (net10 Release, in-process short job, i7-11850H; per-op):
      alloc+free 128B: NativeMemory 49ns / native mi 9.7ns / managed 14.8ns;
      2KB: 44 / 30.5 / 29.5ns; 16KB: 178 / 34.6 / 28.6ns; 256KB: 349 / 32.3 / 30.3ns;
      mixed churn 16KB: 106 / 38 / 34ns; realloc-grow to 16KB: native mi 357ns vs
      managed 318ns; 4-thread 64KB blocks: 80.6ms / 2.33ms / 2.07ms. The managed
      port matches or beats native mimalloc everywhere except the sub-1KB fast
      path (thread-static ctx read overhead), where it is still 3.3x faster than
      NativeMemory.
      FINAL VALIDATION (2026-08-06): Storage suite 352/352 parity with native
      (net8 Debug + net10 Release); Core suite 760/760 green; Acceptance suite
      746/746 green (6 minutes of full stream topologies on the managed
      allocator). THE PORT IS COMPLETE — all 10 tasks done.

## Testing strategy

1. **Unit tests per layer** as it lands (bitmap ops, bin/size-class math vs. C tables, page-map).
2. **Cross-checks against the C implementation:** size-class tables (`mi_bin`, `mi_good_size`)
   asserted against values captured from the native lib (Storage already ships mimalloc.dll for
   win-x64 — can P/Invoke it *in tests only* to compare outputs).
3. **Concurrency stress tests:** producer/consumer free (thread A allocates, B frees), abandonment
   (thread dies with live blocks, other threads reclaim), whole-bitmap contention, aligned realloc
   churn. Run with many iterations; assert allocator invariants (`used`, `capacity`, list heads).
4. **Integration:** swap into `FlowtideMemoryAllocation`, run `FlowtideDotNet.Storage.Tests` and
   `FlowtideDotNet.AcceptanceTests`.

## Implementation notes discovered while porting

- `Interlocked` has no `nint`/`nuint` overloads for And/Or/Add — reinterpret via
  `Unsafe.As<nuint, ulong>(ref p)` (64-bit only port, fine).
- Pointer-typed atomics can't use `ref T*` + `Unsafe.AsPointer` (pointer types are
  invalid generic args, and `&refParam` is CS0212). They take `T**` instead, which
  matches the C call sites (`mi_atomic_load_ptr_acquire(&pq->first)`) exactly.
- Lambdas can capture pointer-typed locals, but cannot take `&` of a captured local
  (CS1686) — put shared slots in native memory in tests.
- `_mi_thread_id()`: must be unique, > 4, bottom 2 bits clear. Do NOT use managed
  thread ids (recycled after thread death + our delayed thread-done ⇒ aliasing hazard).
  Uses a never-reused global counter << 3 in a [ThreadStatic] cache (Prim.cs).
- `mi_lock_t` is a spin-then-yield lock (SpinWait) since OS lock objects can't be
  embedded in native structs; only guards slow paths.
- C `long` fields (theap counters, options) are C# `long` (64-bit everywhere).
- Fixed-size arrays: pointer arrays -> `fixed ulong buf[N]` + cast on access
  (fixed buffers reject pointer element types); struct arrays -> `[InlineArray]`.
- The placeholder structs `mi_bitmap_t`/`mi_bbitmap_t`/`mi_meta_page_t` at the bottom
  of Types.cs must be replaced when Bitmap.cs / ArenaMeta.cs land.

## Session log

- **2026-08-05:** Checked out v3.4.4 (removed stale local branch `v3.4.4` that pointed at
  v3.2.8; work branch is `mimalloc-v3.4.4`). Scaffolded project + tests + slnx wiring.
  Wrote this plan. Ported the foundation layer (Bits, Atomic, Types, Stats structs,
  Internal helpers, Libc messages, Asserts, Prim thread-id); 31 tests green on
  net8.0 + net10.0.
- **2026-08-05 (cont.):** Ported Options.cs (full), stats update functions, Os.cs,
  PrimOs.cs (P/Invoke prim layer; semantics extracted from the C by a 2-agent workflow,
  then hand-verified flag-by-flag), and Bitmap.cs (~1300 lines, full bitmap.c port).
  First adversarial verification workflow over foundation+OS+options confirmed only
  4 minor diagnostics findings (fixed); os.c comparer found zero issues. Second
  verification workflow: BOTH Bitmap.cs comparers found zero issues; one minor
  Windows _mi_prim_free error-code finding confirmed and fixed.
- **2026-08-05 (cont. 2):** Ported Random.cs (chacha20; RFC 8439 vectors green),
  PageMap.cs (2-level page map incl. pre-init empty-map trick), page accessors into
  Internal.cs, minimal Init.cs (_mi_subproc_main), ArenaMeta.cs (with TEMPORARY
  arena stubs -> OS fallback, marked for replacement in task #6). 86 tests green on
  net8.0 Debug + net10.0 Release. Remaining known gaps: aligned-hint randomization
  (wire _mi_os_get_aligned_hint to theap random when init.c lands), thread auto-done
  prims (task #8 init.c).
- **2026-08-05 (cont. 3):** User committed checkpoint (e9829b85). Ported the memory
  half of arena.c to Arena.cs (~1100 lines: ids, try_alloc_at, reserve with
  exponential scaling, forall-arenas iteration, alloc_aligned, free, full purge
  machinery with the atomic guard); replaced the ArenaMeta stubs; wired the main heap
  in Init.cs (heap_main + _mi_subproc + _mi_is_heap_main). 97 tests green on net8.0
  Debug + net10.0 Release (arena roundtrips, immediate+delayed purge, arena-backed
  meta allocator, concurrent alloc/free stress). Adversarial verification (2 finders
  + 1 refuter): ZERO confirmed findings (one numa_node candidate refuted; aligned to
  the C value anyway).
- **2026-08-05 (cont. 4):** User committed checkpoint. Ported the page layer:
  PageQueue.cs, Page.cs, ArenaPages.cs, page-flag/ownership/theap inlines into
  Internal.cs, empty statics + bin-size table + lazy `mi_process_init` into Init.cs
  (the page map now initializes as part of process init, matching C; explicit
  `_mi_page_map_init` calls are idempotent). 111 tests green on net8.0 Debug +
  net10.0 Release: bin function invariants, full page lifecycles through the public
  surface (fresh alloc for all size classes + singleton, block pop/free/collect,
  queue/retire/collect-retired, abandon → find-abandoned reclaim → unabandon
  roundtrips, cross-thread xthread_free push + collect). All intermediate test
  failures during development were WRONG TEST EXPECTATIONS (single-block extends
  for >=4KiB blocks, abandoned-mapped thread-id persisting after bitmap claim,
  unreachable bins 3/5/7 and 61-72) -- the ported code matched C each time.
  Adversarial verification: ArenaPages comparer zero issues; one confirmed minor
  fixed (deferred-free Volatile ordering); static-memid flags aligned. Next:
  task #8 = theap.c/heap.c/threadlocal.c/init.c rest (thread init/done, TLS,
  theap collect), then #9 alloc/free fast paths + _mi_malloc_generic, #10 integration.
- **2026-08-05 (cont. 5):** User committed checkpoint. Task #8: ported `mi_stats_add` +
  `_mi_stats_merge_into` (Stats.cs), `ThreadLocalMi.cs` (threadlocal.c; slots array via
  meta alloc with memid header field — mi_rezalloc lands with task #9), `Theap.cs`
  (theap.c: visit/collect/init/create/incref-decref/free + area/block visiting with the
  fast-divisor bitmap walk), `Heap.cs` (heap.c minus new/delete/destroy + the prim-tls.h
  `_mi_heap_theap*` inlines incl. the real `_mi_page_associated_theap_peek`), and the
  Init.cs completion: per-thread `mi_thread_ctx_t` + finalizer sentinel with id/ctx
  adoption, `_mi_theap_default/cached(_set)`, `_mi_theap_options_init`,
  `mi_tld_alloc/free`, thread init/done (`mi_thread_init_ex`, `mi_thread_theaps_done`
  with the C freed-exchange retry protocol), real `_mi_subproc()`, `mi_heap_main()`,
  `heap->theap = mi_thread_local_key_fast` for the main heap, `_mi_thread_locals_init`
  in process init. 121 tests green on net8.0 Debug + net10.0 Release, including:
  thread init wiring (fast slot, cookie, tld/heap lists), heap-theap caching +
  refcount (1 init + 1 cached), explicit thread-done abandonment of used pages +
  cross-thread reclaim, **finalizer-sentinel abandonment when a thread exits without
  mi_thread_done** (GC-driven), forced theap collect freeing empty pages, 8-thread
  concurrent init with distinct theaps/ids, dynamic TLS key create/free/version
  protection, slots expansion (16→64) and cross-thread isolation. One test
  expectation fixed (reading a FREED key returns the stale value — C semantics;
  only the reused key's version protects). Known C-inherited quirk noted: the
  mi_heap_free_theaps (heap→tld lock order) vs mi_thread_theaps_done (tld→heap)
  inversion is resolved by the freed-exchange short-circuit exactly as in C; with
  only the main heap (Flowtide) mi_heap_free_theaps never runs.
  Adversarial verification (6 finders incl. a dedicated sentinel-concurrency
  reviewer + 2 refuters per finding, 12 agents): threadlocal.c, theap.c and
  stats-merge comparers found ZERO issues; 2 distinct findings confirmed and
  fixed: (1) mi_heap_free_theaps used the non-atomic `__mi_stat_counter_increase`
  where C's heap-stat macro is the atomic `_mt` variant (heap->stats is shared);
  (2) OOM-path divergence — `_mi_theap_default_set` could silently fail if the
  port-only thread-ctx meta-allocation failed AFTER the tld/theap were already
  created+registered (unreachable in C where the TLS store is infallible); fixed
  by creating the ctx at the top of `mi_thread_init_ex` so ctx OOM propagates
  as a clean init failure before anything registers, making the later stores
  infallible (asserted). 121 tests re-run green on both matrices.
- **2026-08-05 (cont. 6):** User committed checkpoint (808ff7f9). Task #9: ported
  `Alloc.cs` (alloc.c), `Free.cs` (free.c incl. the xthread_free CAS push with
  ownership claim and the try-collect free/reclaim/reabandon/unown chain),
  `AllocAligned.cs` (alloc-aligned.c), `mi_find_page` + `_mi_malloc_generic`
  (Page.cs), heap lifecycle `mi_heap_new/delete/destroy` + `_mi_heap_new_for_subproc`
  (Heap.cs), heap-wide block visiting + `_mi_heap_move_pages`/`_mi_heap_destroy_pages`
  + real `mi_arena_pages_alloc` (ArenaPages.cs), `_mi_strlen/strnlen/memset_aligned`
  (Libc.cs), threadlocal.c re-based on `mi_rezalloc`/`mi_free`, and the
  `MI_MEM_HEAP_MAIN` theap-free branch restored via `_mi_free_subproc_safe`.
  158 tests green on net8.0 Debug + net10.0 Release: full write/read/free
  roundtrips across every size class incl. huge singletons, zalloc/rezalloc
  zeroing, realloc reuse-vs-move semantics, aligned alloc for alignments 32B-4MiB
  (natural, over-alloc and huge-singleton paths) + aligned realloc, usable/good
  size equality, cross-thread frees collected via xthread_free, frees into
  ABANDONED pages after thread exit (try-collect path frees the pages), 20k-op
  single-thread churn with stat balance, 6-thread producer/consumer stress with
  cross-thread frees + thread exits, heap new/delete (pages move to main heap,
  blocks stay freeable) and destroy, and heap_visit_blocks. One latent task-#7
  finding fixed along the way (see step 8 note: over-strong asserts for 8-byte
  blocks with separated page meta -- unreachable in C debug builds).
  Adversarial verification (6 finders: alloc/free/alloc-aligned/malloc-generic/
  heap-lifecycle line comparers + a lock-free free-path concurrency reviewer
  covering the xthread_free CAS protocol, unown retry structure, xtid dispatch
  atomicity and collect-partly guarantees): ZERO findings across all six.
  Remaining: task #10 -- public API surface, FlowtideMemoryAllocation
  integration, Flowtide test suites, benchmarks vs native.
- **2026-08-06 (review pass):** Full-port critical review against the C (15 line-by-line
  C-file/C#-file comparers, each finding re-checked by an adversarial verifier). The
  algorithmic core came back CLEAN: zero divergences in Bitmap.cs, ArenaPages.cs,
  Alloc.cs/AllocAligned.cs, Heap.cs/Theap.cs, Types.cs (constants + struct layout) and
  Free.cs. Fixed everything that was confirmed:
  - `_mi_strnlen` (Libc.cs): the `&&` operands were swapped vs libc.c, so it dereferenced
    `s[len]` BEFORE the bound test -- a one-byte out-of-bounds read at `s[max_len]`
    (and at `s[0]` when `max_len == 0`), reachable from the public `mi_strndup`.
  - Three `MI_STAT>0` sites that were dropped (Page.cs): `pages_retire` in
    `_mi_page_retire`, `pages_extended` and `page_committed` in `mi_page_extend_free`.
    page.c:691 is the ONLY producer of `page_committed` in all of mimalloc, so the
    "touched" statistic was permanently zero.
  - `mi_process_init` (Init.cs): the tail of C's `mi_process_init_once` was missing, so
    `MIMALLOC_RESERVE_OS_MEMORY` was parsed and then silently discarded. (The
    `reserve_huge_os_pages` branch stays out of scope -- 1 GiB pages are unsupported.)
  - `MI_MALLOC_VERSION` was 344; mimalloc.h encodes major*10000+minor*100+patch, so
    v3.4.4 is 30404. `MiMallocApi.mi_version()` also hardcoded the literal instead of
    the constant.
  - Option parsing (Options.cs): C's `strtol` leaves `errno == 0` when it converts NO
    digits and resets `end` to the start of the buffer, so a bare size suffix ("KB") is
    accepted with value 0; the port folded that into a parse failure and kept the default.
  - `mi_message_with_thread_prefix` (Options.cs): C's `mi_vfprintf_thread` only inserts
    `thread 0x..:` for NON-main threads; the port always did.
  - Output/error handler globals (Options.cs) are `_Atomic`/`volatile` in C -- now go
    through `Volatile.Read/Write`. `_mi_error_message` snapshots the handler into a local
    (C re-reads the volatile for the call, which would fault if cleared in between).
  - `_mi_options_post_init` / `mi_options_print(_out)` ported, so `MIMALLOC_VERBOSE=1`
    dumps the effective option table again; called at the tail of process init.
  - The `MI_DEBUG>1` `mi_page_is_full => mi_page_is_mostly_used` invariant in
    `mi_free_try_collect_mt` (Free.cs) was dropped; restored.
  - The "runtime-guard against 32-bit" this document promised did not exist: added a
    `PlatformNotSupportedException` in `mi_process_init` when `IntPtr.Size != 8`.
  - **Packaging (release.yml):** FlowtideDotNet.Storage packs as a NuGet package and now
    ProjectReferences MiMalloc, so its nuspec declared a dependency on an
    `FlowtideDotNet.MiMalloc` package that was never published -- every consumer of
    Storage (and transitively Core, the connectors, ...) would fail restore with NU1101.
    Reproduced with a local feed, then fixed by adding a pack step (the publish step
    already globs `*.nupkg`).
  Investigated and REFUTED (kept as-is): `[DllImport("libc")]` resolution on Linux was
  flagged as process-fatal -- verified by running the actual P/Invoke in
  `mcr.microsoft.com/dotnet/sdk:8.0` (Debian/glibc) and `:8.0-alpine` (musl); it resolves
  on both even with no `libc.so` file present. Also refuted as config-driven or
  unreachable: meta-page 64-byte alignment (the port computes the offset at runtime),
  `mi_arena_purge_delay` `long` width, unix `large_page_size = 0`, `needs_recommit`,
  the page-map init assert, and `mi_atomic_do_once`.
  Verification: MiMalloc 158/158 on net8.0 Debug + net10.0 Release; Storage 352 tests
  with the SAME failures as the pre-change baseline (7 pre-existing golden-file failures
  on net10 Release, 1 on net8 Debug -- confirmed by stashing the fixes and re-running).
