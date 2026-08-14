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
// Port of the mimalloc v3.4.4 OS primitives (`src/prim/windows/prim.c` and
// `src/prim/unix/prim.c`) to P/Invoke. Original: Copyright (c) 2018-2026
// Microsoft Research, Daan Leijen (MIT license).
//
// Scope (see README.md): modern Windows 10+/Linux (glibc+musl)/macOS only. Large
// (2MiB) and huge (1GiB) OS pages are not supported (large_page_size stays 0 which
// disables every large-page branch downstream; huge-page alloc returns ENOMEM like
// the C fallback stub). Return convention: 0 on success, else the raw OS error
// (Win32 error code / errno) — never throws.

using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        // ---------------------------------------------------------------------------
        // Shared prims
        // ---------------------------------------------------------------------------

        // The port never runs before the runtime is initialized.
        public static bool _mi_preloading() => false;

        // Clock ticks (milliseconds, monotonic; epoch is arbitrary)
        public static long _mi_prim_clock_now() => Environment.TickCount64;

        // from stats.c
        public static long _mi_clock_now() => _mi_prim_clock_now();
        public static long _mi_clock_start() => _mi_clock_now();
        public static long _mi_clock_end(long start) => _mi_clock_now() - start;

        // Fill a buffer with strong randomness; return `false` on error.
        public static bool _mi_prim_random_buf(void* buf, nuint buf_len)
        {
            try
            {
                RandomNumberGenerator.Fill(new Span<byte>(buf, checked((int)buf_len)));
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static nuint _mi_prim_numa_node() => 0;
        public static nuint _mi_prim_numa_node_count() => 1;

        // Is this thread part of a thread pool? In C this matters because OS threadpool
        // threads may never run TLS destructors; the .NET ThreadPool has the same
        // property (worker threads are recycled, not exited), so map it directly.
        public static bool _mi_prim_thread_is_in_threadpool() => Thread.CurrentThread.IsThreadPoolThread;

        public static void _mi_prim_thread_yield() => Thread.Yield();

        public static void _mi_prim_mem_init(ref mi_os_mem_config_t config)
        {
            if (OperatingSystem.IsWindows())
            {
                mi_prim_mem_init_windows(ref config);
            }
            else
            {
                mi_prim_mem_init_unix(ref config);
            }
        }

        public static int _mi_prim_free(void* addr, nuint size)
            => OperatingSystem.IsWindows() ? mi_prim_free_windows(addr, size) : mi_prim_free_unix(addr, size);

        public static int _mi_prim_alloc(void* hint_addr, nuint size, nuint try_alignment, bool commit, bool allow_large, ref bool is_large, ref bool is_zero, void** addr)
        {
            mi_assert_internal(size > 0 && (size % _mi_os_page_size()) == 0);
            mi_assert_internal(commit || !allow_large);
            mi_assert_internal(try_alignment > 0);
            return OperatingSystem.IsWindows()
                ? mi_prim_alloc_windows(hint_addr, size, try_alignment, commit, ref is_large, ref is_zero, addr)
                : mi_prim_alloc_unix(hint_addr, size, try_alignment, commit, ref is_large, ref is_zero, addr);
        }

        public static int _mi_prim_commit(void* addr, nuint size, ref bool is_zero)
            => OperatingSystem.IsWindows() ? mi_prim_commit_windows(addr, size, ref is_zero) : mi_prim_commit_unix(addr, size, ref is_zero);

        public static int _mi_prim_decommit(void* addr, nuint size, ref bool needs_recommit)
            => OperatingSystem.IsWindows() ? mi_prim_decommit_windows(addr, size, ref needs_recommit) : mi_prim_decommit_unix(addr, size, ref needs_recommit);

        public static int _mi_prim_reset(void* addr, nuint size)
            => OperatingSystem.IsWindows() ? mi_prim_reset_windows(addr, size) : mi_prim_reset_unix(addr, size);

        public static int _mi_prim_reuse(void* addr, nuint size)
            => OperatingSystem.IsWindows() ? 0 : mi_prim_reuse_unix(addr, size);

        public static int _mi_prim_protect(void* addr, nuint size, bool protect)
            => OperatingSystem.IsWindows() ? mi_prim_protect_windows(addr, size, protect) : mi_prim_protect_unix(addr, size, protect);

        // Huge (1GiB) pages are not ported: replicate the C fallback stub exactly
        // (addr = null, is_zero = false, return ENOMEM) so callers degrade gracefully.
        public static int _mi_prim_alloc_huge_os_pages(void* hint_addr, nuint size, int numa_node, ref bool is_zero, void** addr)
        {
            is_zero = false;
            *addr = null;
            return ENOMEM;
        }

        // ---------------------------------------------------------------------------
        // Windows (src/prim/windows/prim.c)
        // ---------------------------------------------------------------------------

        private const uint MEM_COMMIT = 0x1000;
        private const uint MEM_RESERVE = 0x2000;
        private const uint MEM_DECOMMIT = 0x4000;
        private const uint MEM_RELEASE = 0x8000;
        private const uint MEM_RESET = 0x80000;
        private const uint PAGE_NOACCESS = 0x01;
        private const uint PAGE_READWRITE = 0x04;

        private const int ERROR_NOT_ENOUGH_MEMORY = 8;
        private const int ERROR_INVALID_ADDRESS = 487;
        private const int ERROR_COMMITMENT_MINIMUM = 635;
        private const int ERROR_PAGEFILE_QUOTA = 1454;
        private const int ERROR_COMMITMENT_LIMIT = 1455;

        [StructLayout(LayoutKind.Sequential)]
        private struct SYSTEM_INFO
        {
            public uint dwOemId;
            public uint dwPageSize;
            public void* lpMinimumApplicationAddress;
            public void* lpMaximumApplicationAddress;
            public nuint dwActiveProcessorMask;
            public uint dwNumberOfProcessors;
            public uint dwProcessorType;
            public uint dwAllocationGranularity;
            public ushort wProcessorLevel;
            public ushort wProcessorRevision;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct MEMORY_BASIC_INFORMATION
        {
            public void* BaseAddress;
            public void* AllocationBase;
            public uint AllocationProtect;
            public ushort PartitionId;
            public nuint RegionSize;
            public uint State;
            public uint Protect;
            public uint Type;
        }

        // MEM_EXTENDED_PARAMETER for VirtualAlloc2: 8-byte Type field (low 8 bits used)
        // followed by an 8-byte union (we use the Pointer member).
        [StructLayout(LayoutKind.Sequential)]
        private struct MEM_EXTENDED_PARAMETER
        {
            public ulong Type;
            public void* Pointer;
        }

        private const ulong MiMemExtendedParameterAddressRequirements = 1;

        [StructLayout(LayoutKind.Sequential)]
        private struct MEM_ADDRESS_REQUIREMENTS
        {
            public void* LowestStartingAddress;
            public void* HighestEndingAddress;
            public nuint Alignment;
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern void* VirtualAlloc(void* lpAddress, nuint dwSize, uint flAllocationType, uint flProtect);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern int VirtualFree(void* lpAddress, nuint dwSize, uint dwFreeType);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern nuint VirtualQuery(void* lpAddress, MEMORY_BASIC_INFORMATION* lpBuffer, nuint dwLength);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern int VirtualProtect(void* lpAddress, nuint dwSize, uint flNewProtect, uint* lpflOldProtect);

        [DllImport("kernel32.dll")]
        private static extern void GetSystemInfo(SYSTEM_INFO* lpSystemInfo);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern int GetPhysicallyInstalledSystemMemory(ulong* TotalMemoryInKilobytes);

        [DllImport("kernel32.dll")]
        private static extern nint GetCurrentProcess();

        // VirtualAlloc2 lives in kernelbase.dll (present since Win10 1803); if the entry
        // point is missing we fall back permanently (sticky flag) like the C GetProcAddress probe.
        [DllImport("kernelbase.dll", SetLastError = true, EntryPoint = "VirtualAlloc2")]
        private static extern void* VirtualAlloc2(nint Process, void* BaseAddress, nuint Size, uint AllocationType, uint PageProtection, MEM_EXTENDED_PARAMETER* ExtendedParameters, uint ParameterCount);

        private static int mi_win_virtual_alloc2_available = 0;   // 0 = unknown, 1 = yes, -1 = no
        private static nuint win_allocation_granularity = 64 * MI_KiB;

        private static void mi_prim_mem_init_windows(ref mi_os_mem_config_t config)
        {
            config.has_overcommit = false;
            config.has_partial_free = false;   // VirtualAlloc regions must be freed as a whole
            config.has_virtual_reserve = true;
            // get the page size
            SYSTEM_INFO si = default;
            GetSystemInfo(&si);
            if (si.dwPageSize > 0) { config.page_size = si.dwPageSize; }
            if (si.dwAllocationGranularity > 0)
            {
                config.alloc_granularity = si.dwAllocationGranularity;
                win_allocation_granularity = si.dwAllocationGranularity;
            }
            // get virtual address bits: index of the highest set bit + 1 (47 on x64)
            if ((nuint)si.lpMaximumApplicationAddress > 0)
            {
                nuint vbits = (nuint)MI_SIZE_BITS - mi_clz((nuint)si.lpMaximumApplicationAddress);
                config.virtual_address_bits = vbits;
            }
            // get physical memory
            ulong memInKiB = 0;
            if (GetPhysicallyInstalledSystemMemory(&memInKiB) != 0 && memInKiB > 0)
            {
                config.physical_memory_in_kib = (nuint)memInKiB;
            }
            // note: large OS pages (SeLockMemoryPrivilege / GetLargePageMinimum) are not
            // ported; config.large_page_size stays 0 which disables large-page use.
        }

        private static int mi_prim_free_windows(void* addr, nuint size)
        {
            // size is ignored: MEM_RELEASE requires dwSize == 0 and the exact base address
            int err = 0;
            if (VirtualFree(addr, 0, MEM_RELEASE) == 0)
            {
                err = Marshal.GetLastWin32Error();
            }
            if (err == ERROR_INVALID_ADDRESS)
            {
                // In mi_os_mem_alloc_aligned the fallback path may have returned a pointer
                // inside the original allocation. Free it with the original base pointer,
                // but only if it is close by (< 4MiB) to guard against wild pointers.
                MEMORY_BASIC_INFORMATION info = default;
                if (VirtualQuery(addr, &info, (nuint)sizeof(MEMORY_BASIC_INFORMATION)) == 0)
                {
                    // C: a failing VirtualQuery makes its error the returned code
                    err = Marshal.GetLastWin32Error();
                }
                else if (info.AllocationBase < addr
                    && (nuint)((byte*)addr - (byte*)info.AllocationBase) < (nuint)(4 * MI_MiB))
                {
                    err = 0;
                    if (VirtualFree(info.AllocationBase, 0, MEM_RELEASE) == 0)
                    {
                        err = Marshal.GetLastWin32Error();
                    }
                }
            }
            return err;
        }

        private static void* win_virtual_alloc_prim_once(void* addr, nuint size, nuint try_alignment, uint flags)
        {
            // on 64-bit systems, try to use the virtual address area after 2TiB for 4MiB aligned allocations
            if (addr == null)
            {
                void* hint = _mi_os_get_aligned_hint(try_alignment, size);
                if (hint != null)
                {
                    void* hp = VirtualAlloc(hint, size, flags, PAGE_READWRITE);
                    if (hp != null) return hp;
                    _mi_verbose_message($"warning: unable to allocate hinted aligned OS memory ({size} bytes, error code: 0x{Marshal.GetLastWin32Error():x}, address: 0x{(nuint)hint:x}, alignment: {try_alignment}, flags: 0x{flags:x})");
                    // fall through on error
                }
            }
            // on modern Windows try use VirtualAlloc2 for aligned allocation
            if (addr == null && try_alignment > win_allocation_granularity && (try_alignment % _mi_os_page_size()) == 0
                && Volatile.Read(ref mi_win_virtual_alloc2_available) >= 0)
            {
                MEM_ADDRESS_REQUIREMENTS reqs = default;
                reqs.Alignment = try_alignment;
                MEM_EXTENDED_PARAMETER param = default;
                param.Type = MiMemExtendedParameterAddressRequirements;
                param.Pointer = &reqs;
                try
                {
                    void* p2 = VirtualAlloc2(GetCurrentProcess(), addr, size, flags, PAGE_READWRITE, &param, 1);
                    Volatile.Write(ref mi_win_virtual_alloc2_available, 1);
                    if (p2 != null) return p2;
                    _mi_warning_message($"unable to allocate aligned OS memory (0x{size:x} bytes, error code: 0x{Marshal.GetLastWin32Error():x}, address: 0x{(nuint)addr:x}, alignment: 0x{try_alignment:x}, flags: 0x{flags:x})");
                    // fall through on error
                }
                catch (EntryPointNotFoundException)
                {
                    Volatile.Write(ref mi_win_virtual_alloc2_available, -1);
                }
                catch (DllNotFoundException)
                {
                    Volatile.Write(ref mi_win_virtual_alloc2_available, -1);
                }
            }
            // last resort
            return VirtualAlloc(addr, size, flags, PAGE_READWRITE);
        }

        private static bool win_is_out_of_memory_error(int err)
        {
            switch (err)
            {
                case ERROR_COMMITMENT_MINIMUM:
                case ERROR_COMMITMENT_LIMIT:
                case ERROR_PAGEFILE_QUOTA:
                case ERROR_NOT_ENOUGH_MEMORY:
                    return true;
                default:
                    return false;
            }
        }

        private static void* win_virtual_alloc_prim(void* addr, nuint size, nuint try_alignment, uint flags)
        {
            long max_retry_msecs = mi_option_get_clamp(mi_option_t.mi_option_retry_on_oom, 0, 2000);  // at most 2 seconds
            if (max_retry_msecs == 1) { max_retry_msecs = 100; }  // if one sets the option to "true"
            for (long tries = 1; tries <= 10; tries++)            // try at most 10 times (=2200ms)
            {
                void* p = win_virtual_alloc_prim_once(addr, size, try_alignment, flags);
                if (p != null)
                {
                    // success, return the address
                    return p;
                }
                else if (max_retry_msecs > 0 && (try_alignment <= 8 * MI_MiB) &&
                         (flags & MEM_COMMIT) != 0 &&
                         win_is_out_of_memory_error(Marshal.GetLastWin32Error()))
                {
                    // if committing regular memory and being out-of-memory,
                    // keep trying for a bit in case memory frees up after all. See issue #894
                    _mi_warning_message($"out-of-memory on OS allocation, try again... (attempt {tries}, 0x{size:x} bytes, error code: 0x{Marshal.GetLastWin32Error():x}, address: 0x{(nuint)addr:x}, alignment: 0x{try_alignment:x}, flags: 0x{flags:x})");
                    long sleep_msecs = tries * 40;  // increasing waits
                    if (sleep_msecs > max_retry_msecs) { sleep_msecs = max_retry_msecs; }
                    max_retry_msecs -= sleep_msecs;
                    Thread.Sleep((int)sleep_msecs);
                }
                else
                {
                    // otherwise return with an error
                    break;
                }
            }
            return null;
        }

        private static int mi_prim_alloc_windows(void* hint_addr, nuint size, nuint try_alignment, bool commit, ref bool is_large, ref bool is_zero, void** addr)
        {
            is_zero = true;
            uint flags = MEM_RESERVE;
            if (commit) { flags |= MEM_COMMIT; }
            // note: no large-page (MEM_LARGE_PAGES) branch in the port (large_page_size == 0)
            is_large = false;
            *addr = win_virtual_alloc_prim(hint_addr, size, try_alignment, flags);
            return *addr != null ? 0 : Marshal.GetLastWin32Error();
        }

        private static int mi_prim_commit_windows(void* addr, nuint size, ref bool is_zero)
        {
            // zero'ing only happens on an initial commit, so we cannot assume the memory is
            // zero when recommitting already-committed or MEM_RESET pages (see C source).
            is_zero = false;
            void* p = VirtualAlloc(addr, size, MEM_COMMIT, PAGE_READWRITE);
            if (p == null) return Marshal.GetLastWin32Error();
            return 0;
        }

        private static int mi_prim_decommit_windows(void* addr, nuint size, ref bool needs_recommit)
        {
            bool ok = VirtualFree(addr, size, MEM_DECOMMIT) != 0;
            needs_recommit = true;  // for safety, assume always decommitted even in the case of an error.
            return ok ? 0 : Marshal.GetLastWin32Error();
        }

        private static int mi_prim_reset_windows(void* addr, nuint size)
        {
            void* p = VirtualAlloc(addr, size, MEM_RESET, PAGE_READWRITE);
            mi_assert_internal(p == addr);
            if (p == null) return Marshal.GetLastWin32Error();
            return 0;
        }

        private static int mi_prim_protect_windows(void* addr, nuint size, bool protect)
        {
            uint oldprotect = 0;
            bool ok = VirtualProtect(addr, size, protect ? PAGE_NOACCESS : PAGE_READWRITE, &oldprotect) != 0;
            return ok ? 0 : Marshal.GetLastWin32Error();
        }

        // ---------------------------------------------------------------------------
        // Unix: Linux + macOS (src/prim/unix/prim.c)
        // ---------------------------------------------------------------------------

        private const int PROT_NONE = 0;
        private const int PROT_READ = 1;
        private const int PROT_WRITE = 2;

        private const int MAP_PRIVATE = 0x02;
        private static readonly int MAP_ANONYMOUS = OperatingSystem.IsMacOS() ? 0x1000 : 0x20;
        private static readonly int MAP_NORESERVE = OperatingSystem.IsMacOS() ? 0 : 0x4000;

        private const int MADV_DONTNEED = 4;                                              // same value on Linux and macOS
        private static readonly int MADV_FREE = OperatingSystem.IsMacOS() ? 5 : 8;        // differs per OS!
        private const int MADV_FREE_REUSABLE = 7;   // macOS only
        private const int MADV_FREE_REUSE = 8;      // macOS only

        private const int EINTR = 4;
        // EAGAIN is 11 on Linux, 35 on macOS
        private static readonly int OS_EAGAIN = OperatingSystem.IsMacOS() ? 35 : 11;

        [DllImport("libc", SetLastError = true)]
        private static extern void* mmap(void* addr, nuint length, int prot, int flags, int fd, nint offset);

        [DllImport("libc", SetLastError = true)]
        private static extern int munmap(void* addr, nuint length);

        [DllImport("libc", SetLastError = true)]
        private static extern int mprotect(void* addr, nuint len, int prot);

        [DllImport("libc", SetLastError = true)]
        private static extern int madvise(void* addr, nuint length, int advice);

        [DllImport("libc", SetLastError = true)]
        private static extern int prctl(int option, ulong arg2, ulong arg3, ulong arg4, ulong arg5);

        private const int PR_SET_THP_DISABLE = 41;
        private const int PR_GET_THP_DISABLE = 42;

        private static readonly void* MAP_FAILED = (void*)(-1);

        private static bool unix_detect_overcommit()
        {
            bool os_overcommit = true;
            if (OperatingSystem.IsLinux())
            {
                try
                {
                    string val = File.ReadAllText("/proc/sys/vm/overcommit_memory").Trim();
                    os_overcommit = val == "0" || val == "1";
                }
                catch
                {
                    // failed to read: assume overcommit (Linux default)
                }
            }
            // macOS: keep the default (true), as in the C code
            return os_overcommit;
        }

        private static bool unix_detect_thp()
        {
            if (OperatingSystem.IsLinux())
            {
                try
                {
                    string val = File.ReadAllText("/sys/kernel/mm/transparent_hugepage/enabled");
                    return val.Contains("[always]") || val.Contains("[madvise]");
                }
                catch
                {
                    return false;
                }
            }
            return false;
        }

        private static void mi_prim_mem_init_unix(ref mi_os_mem_config_t config)
        {
            nuint psize = (nuint)Environment.SystemPageSize;   // == sysconf(_SC_PAGESIZE)
            if (psize > 0)
            {
                config.page_size = psize;
                config.alloc_granularity = psize;   // on Unix alloc_granularity == page_size
                ulong physBytes = (ulong)GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
                if (physBytes > 0) { config.physical_memory_in_kib = (nuint)(physBytes / 1024); }
            }
            config.large_page_size = 0;   // large (2MiB) pages not ported
            config.has_overcommit = unix_detect_overcommit();
            config.has_partial_free = true;    // mmap can free in parts
            config.has_virtual_reserve = true;
            config.has_transparent_huge_pages = unix_detect_thp();
            // virtual address bits: 47 on x64, 48 on arm64 (see bits.h MI_MAX_VABITS)
            config.virtual_address_bits = RuntimeInformation.ProcessArchitecture == Architecture.X64 ? 47u : 48u;

            // disable transparent huge pages for this process?
            if (OperatingSystem.IsLinux() && !mi_option_is_enabled(mi_option_t.mi_option_allow_thp))
            {
                config.has_transparent_huge_pages = false;
                try
                {
                    if (prctl(PR_GET_THP_DISABLE, 0, 0, 0, 0) == 0)   // -1 on error, 1 if already disabled
                    {
                        prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0);
                    }
                }
                catch
                {
                    // prctl unavailable: ignore (hardening only)
                }
            }
        }

        private static int mi_prim_free_unix(void* addr, nuint size)
        {
            if (size == 0) return 0;
            bool err = munmap(addr, size) == -1;
            return err ? Marshal.GetLastPInvokeError() : 0;
        }

        // return errno on failure
        private static int unix_madvise(void* addr, nuint size, int advice)
        {
            int res = madvise(addr, size, advice);   // linux returns -1 on failure and sets errno
            return res == 0 ? 0 : Marshal.GetLastPInvokeError();
        }

        private static void* unix_mmap_prim(void* addr, nuint size, int protect_flags, int flags, int fd)
        {
            void* p = mmap(addr, size, protect_flags, flags, fd, 0 /* offset */);
            // note: the Linux PR_SET_VMA_ANON_NAME "mimalloc" region naming is diagnostic
            // only and is skipped in the port.
            return p;
        }

        private static void* unix_mmap_prim_aligned(void* addr, nuint size, nuint try_alignment, int protect_flags, int flags, int fd)
        {
            void* p;
            // on 64-bit systems, use the virtual address area after 2TiB for 4MiB aligned allocations
            if (addr == null)
            {
                void* hint = _mi_os_get_aligned_hint(try_alignment, size);
                if (hint != null)
                {
                    p = unix_mmap_prim(hint, size, protect_flags, flags, fd);
                    if (p == MAP_FAILED || !_mi_is_aligned(p, try_alignment))
                    {
                        int err = Marshal.GetLastPInvokeError();
                        _mi_trace_message($"unable to directly request hinted aligned OS memory (error: {err} (0x{err:x}), size: 0x{size:x} bytes, alignment: 0x{try_alignment:x}, hint address: 0x{(nuint)hint:x})");
                    }
                    if (p != MAP_FAILED) return p;
                    // fall back to regular mmap
                }
            }
            // regular mmap
            p = unix_mmap_prim(addr, size, protect_flags, flags, fd);
            if (p != MAP_FAILED) return p;
            // failed to allocate
            return null;
        }

        private static void* unix_mmap(void* addr, nuint size, nuint try_alignment, int protect_flags, bool large_only, bool allow_large, ref bool is_large)
        {
            int fd = -1;   // note: macOS VM_MAKE_TAG region tagging is diagnostic only; skipped
            int flags = MAP_PRIVATE | MAP_ANONYMOUS;
            if (_mi_os_has_overcommit())
            {
                flags |= MAP_NORESERVE;
            }
            // note: no huge/large page branch in the port (MI_OS_HAS_HUGE_PAGES disabled;
            // large_page_size == 0 so _mi_os_canuse_large_page is always false)
            is_large = false;
            return unix_mmap_prim_aligned(addr, size, try_alignment, protect_flags, flags, fd);
        }

        private static int mi_prim_alloc_unix(void* hint_addr, nuint size, nuint try_alignment, bool commit, ref bool is_large, ref bool is_zero, void** addr)
        {
            is_zero = true;
            int protect_flags = commit ? (PROT_WRITE | PROT_READ) : PROT_NONE;
            *addr = unix_mmap(hint_addr, size, try_alignment, protect_flags, false, false, ref is_large);
            return *addr != null ? 0 : Marshal.GetLastPInvokeError();
        }

        private static int mi_prim_commit_unix(void* addr, nuint size, ref bool is_zero)
        {
            // commit: ensure we can access the area
            // note: we may think that *is_zero can be true since the memory is either
            // fresh or was committed and decommitted before (with MADV_DONTNEED).
            // However, some kernel versions or systems may have the memory not zeroed out
            // in that case; also commit may be called on a partially committed range with
            // live data, so we conservatively say it is not zero'd (as in the C code).
            is_zero = false;
            int err = 0;
            if (mprotect(addr, size, PROT_READ | PROT_WRITE) != 0)
            {
                err = Marshal.GetLastPInvokeError();
            }
            return err;
        }

        private static int mi_prim_decommit_unix(void* addr, nuint size, ref bool needs_recommit)
        {
            int err;
            if (OperatingSystem.IsMacOS())
            {
                // macOS: MADV_FREE_REUSABLE updates the process footprint immediately (issue #1097);
                // fall back to MADV_DONTNEED on error
                err = unix_madvise(addr, size, MADV_FREE_REUSABLE);
                if (err != 0)
                {
                    err = unix_madvise(addr, size, MADV_DONTNEED);
                }
            }
            else
            {
                // decommit: use MADV_DONTNEED as it decreases rss immediately
                err = unix_madvise(addr, size, MADV_DONTNEED);
            }
            // release build behavior: the memory stays accessible and reads back as zero
            // on next touch, so it needs no explicit recommit (C: !MI_DEBUG && MI_SECURE<=2)
            needs_recommit = false;
            return err;
        }

        private static int mi_advice_for_reset = 0;   // 0 = uninitialized; sticky downgrade from MADV_FREE to MADV_DONTNEED

        private static int mi_prim_reset_unix(void* addr, nuint size)
        {
            // We try to use `MADV_FREE` as that is the fastest. A drawback though is that it
            // will not reduce the `rss` stats immediately.
            int advice = Volatile.Read(ref mi_advice_for_reset);
            if (advice == 0) { advice = MADV_FREE; }
            int err = unix_madvise(addr, size, advice);
            while (err == OS_EAGAIN) { err = unix_madvise(addr, size, advice); }   // retry on EAGAIN
            if (err == EINVAL && advice == MADV_FREE)
            {
                // if MADV_FREE is not supported, fall back to MADV_DONTNEED from now on
                Volatile.Write(ref mi_advice_for_reset, MADV_DONTNEED);
                err = unix_madvise(addr, size, MADV_DONTNEED);
            }
            return err;
        }

        private static int mi_prim_reuse_unix(void* addr, nuint size)
        {
            if (OperatingSystem.IsMacOS())
            {
                // balance the earlier MADV_FREE_REUSABLE from decommit (footprint accounting)
                return unix_madvise(addr, size, MADV_FREE_REUSE);
            }
            return 0;
        }

        private static int mi_prim_protect_unix(void* addr, nuint size, bool protect)
        {
            int err = 0;
            if (mprotect(addr, size, protect ? PROT_NONE : (PROT_READ | PROT_WRITE)) != 0)
            {
                err = Marshal.GetLastPInvokeError();
            }
            return err;
        }
    }
}
