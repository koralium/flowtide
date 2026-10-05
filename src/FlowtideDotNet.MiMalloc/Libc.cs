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
// Port of the parts of mimalloc v3.4.4 `src/libc.c` and the message functions from
// `src/options.c` that the rest of the port needs. Original: Copyright (c) 2018-2024
// Microsoft Research, Daan Leijen (MIT license).
//
// The C code carries its own printf/memcpy implementations to avoid recursing into
// malloc; in .NET we can safely use NativeMemory/Console (no allocator recursion:
// the port never hooks the managed allocator). Message formatting happens at call
// sites with interpolated strings instead of printf formats.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        // ---------------------------------------------------------------------------
        // memory fill/copy helpers (C: _mi_memzero / _mi_memcpy / _mi_memset)
        // ---------------------------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void _mi_memzero(void* dst, nuint size) => NativeMemory.Clear(dst, size);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void _mi_memset(void* dst, byte val, nuint size) => NativeMemory.Fill(dst, size, val);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void _mi_memcpy(void* dst, void* src, nuint size) => NativeMemory.Copy(src, dst, size);

        // aligned variants (the C code uses these to give the compiler alignment info;
        // in .NET the plain versions are already optimal)
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void _mi_memzero_aligned(void* dst, nuint size) => NativeMemory.Clear(dst, size);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void _mi_memcpy_aligned(void* dst, void* src, nuint size) => NativeMemory.Copy(src, dst, size);

        // C: _mi_memset_aligned (used for the MI_DEBUG freed/uninit fill patterns)
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void _mi_memset_aligned(void* dst, byte val, nuint size) => NativeMemory.Fill(dst, size, val);

        // ---------------------------------------------------------------------------
        // string helpers (C: libc.c `_mi_strlen` / `_mi_strnlen`)
        // ---------------------------------------------------------------------------

        public static nuint _mi_strlen(byte* s)
        {
            if (s == null) return 0;
            nuint len = 0;
            while (s[len] != 0) { len++; }
            return len;
        }

        public static nuint _mi_strnlen(byte* s, nuint max_len)
        {
            if (s == null) return 0;
            nuint len = 0;
            // note: the bound MUST be tested before the dereference (as in the C), otherwise
            // `s[max_len]` is read -- one byte past the caller's buffer.
            while (len < max_len && s[len] != 0) { len++; }
            return len;
        }

        // note: the message/error functions (_mi_error_message, _mi_warning_message,
        // _mi_verbose_message, _mi_trace_message) live in Options.cs (as in the C code).

        public static void _mi_fatal_error(string message)
        {
            Console.Error.WriteLine($"mimalloc: fatal: {message}");
            throw new InvalidOperationException($"mimalloc fatal error: {message}");
        }
    }
}
