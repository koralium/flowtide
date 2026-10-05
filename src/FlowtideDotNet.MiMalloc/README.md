# FlowtideDotNet.MiMalloc

C# port of [mimalloc](https://github.com/microsoft/mimalloc) v3.4.4 (tag `v3.4.4`, commit `1f06f694`).
Used as the allocator in FlowtideDotNet.Storage, so no native mimalloc binary is needed.

The port follows the C source closely: one C# file per C file, same function and struct names,
so it can be diffed side by side against the C when upgrading or debugging. All allocator
metadata lives in native memory, the GC never sees it. The `MIMALLOC_*` environment variables
work the same as in native mimalloc.

## Pinned configuration

The C code has many compile-time variants, this port pins one:

| Setting | Value |
|---|---|
| Word size | 64-bit only (checked at init) |
| `MI_SECURE`, `MI_PADDING`, `MI_ENCODE_FREELIST`, `MI_GUARDED` | 0 |
| `MI_DEBUG` | on in Debug builds (asserts via `Debug.Assert`), 0 in Release |
| `MI_STAT` | 1 |
| `MI_PAGE_MAP_FLAT` | 0 (two-level page map) |
| `MI_PAGE_META_IS_SEPARATED` | 1 |
| Arena slice | 64 KiB (`MI_ARENA_SLICE_SHIFT` 16), bitmap chunks 512 bits |
| Bins | `MI_BIN_HUGE` 73, `MI_ALIGN2W` (max align 16) |
| OS large/huge pages, NUMA, sub-processes | not supported (single subproc, single NUMA node) |

## C to C# mapping

- Atomics: `mi_atomic_*` helpers over `Interlocked`/`Volatile`. Orderings are equal or stronger
  than the C, weak CAS is strong CAS. Pointer atomics take `T**` since pointer types can't be
  generic arguments.
- Thread ids come from a never-reused global counter (`<< 3`), not managed thread ids, since
  those are recycled and thread cleanup here is GC-delayed.
- Thread exit: all per-thread state sits in one native `mi_thread_ctx_t` behind a `[ThreadStatic]`,
  and a finalizable sentinel runs `mi_thread_done` on the finalizer thread after the thread dies,
  temporarily adopting the dead thread's id so the C same-thread cleanup logic runs unchanged.
- Fixed arrays: pointer arrays as `fixed ulong` + cast, struct arrays as `[InlineArray]`,
  variable-length bitmap tails through pointer accessors.
- `mi_lock_t` is an 8-byte spinlock (C uses a 40-byte pthread mutex, only used on slow paths).
- OS layer P/Invokes `VirtualAlloc`/`mmap` directly, `NativeMemory` cannot do reserve-only.

## Not ported

- `mi_realpath`, the C++ `mi_new_*` family, `mi_subproc_new/destroy`, heap tags.
- Message formatting uses interpolated strings with gate-first handlers instead of printf,
  and the output path swallows exceptions (C's `fputs` cannot fail, and these run inside
  allocator failure paths).

## Verifying against the C

Check out the mimalloc tag above and diff file by file (`bitmap.c` vs `Bitmap.cs` and so on).
Tests run on net8.0 Debug (asserts on) and net10.0 Release, plus the Storage/Core/Acceptance
suites through `FlowtideMemoryAllocation`.
