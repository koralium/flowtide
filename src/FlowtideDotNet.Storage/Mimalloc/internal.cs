using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace mimalloctests
{
    public unsafe partial class MiMalloc
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool mi_unlikely(bool x) => x;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool mi_likely(bool x) => x;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static mi_memid_t _mi_memid_create(mi_memkind_t memkind)
        {
            mi_memid_t memid = default;
            memid.memkind = memkind;
            return memid;
        }

        private static void _mi_assert_fail(string assertion, string fname, uint line, string func)
        {
            Console.WriteLine("mimalloc: assertion failed: at \"{0}\":{1}, {2}\n  assertion: \"{3}\"", fname, line, (func is null) ? "" : func, assertion);
            //_mi_fprintf(null, null, "mimalloc: assertion failed: at \"{0}\":{1}, {2}\n  assertion: \"{3}\"\n", fname, line, (func is null) ? "" : func, assertion);
            abort();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void abort() => Environment.Exit(-1);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void mi_assert_internal(bool expr, [CallerArgumentExpression(nameof(expr))] string assertion = "", [CallerFilePath] string fname = "", [CallerLineNumber] uint line = 0, [CallerMemberName] string func = "")
        {
            if (MI_DEBUG > 1)
            {
                mi_assert(expr, assertion, fname, line, func);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void mi_assert(bool expr, [CallerArgumentExpression(nameof(expr))] string assertion = "", [CallerFilePath] string fname = "", [CallerLineNumber] uint line = 0, [CallerMemberName] string func = "")
        {
            if ((MI_DEBUG != 0) && !expr)
            {
                _mi_assert_fail(assertion, fname, line, func);
            }
        }

    }
}
