//using System;
//using System.Collections.Generic;
//using System.Text;

//namespace mimalloctests
//{
//    internal class atomic
//    {
//        // --------------------------------------------------------------------------------------------
//        // CAS (Compare-And-Swap)
//        // C# Interlocked.CompareExchange returnerar det gamla värdet. C++ förväntar sig en bool och uppdaterar expected.
//        // Vi skapar en C#-vänlig "Strong/Weak"-metod som beter sig som C11.
//        // --------------------------------------------------------------------------------------------

//        public static bool CasWeak(ref nuint location, ref nuint expected, nuint desired) =>
//            CasStrong(ref location, ref expected, desired);

//        public static bool CasStrong(ref nuint location, ref nuint expected, nuint desired)
//        {
//            nuint read = Interlocked.CompareExchange(ref location, desired, expected);
//            if (read == expected)
//            {
//                return true;
//            }
//            expected = read;
//            return false;
//        }

//        public static bool CasStrong64(ref long location, ref long expected, long desired)
//        {
//            long read = Interlocked.CompareExchange(ref location, desired, expected);
//            if (read == expected)
//            {
//                return true;
//            }
//            expected = read;
//            return false;
//        }

//        // --------------------------------------------------------------------------------------------
//        // Load / Store
//        // Relaxed kan hanteras med standardläsningar (eller Volatile), Acquire/Release styrs via Volatile.
//        // --------------------------------------------------------------------------------------------

//        public static nuint LoadAcquire(ref nuint location) => Volatile.Read(ref location);
//        public static nuint LoadRelaxed(ref nuint location) => Volatile.Read(ref location);

//        public static void StoreRelease(ref nuint location, nuint value) => Volatile.Write(ref location, value);
//        public static void StoreRelaxed(ref nuint location, nuint value) => Volatile.Write(ref location, value);

//        public static long Load64Acquire(ref long location) => Volatile.Read(ref location);
//        public static long Load64Relaxed(ref long location) => Volatile.Read(ref location);

//        public static void Store64Release(ref long location, long value) => Volatile.Write(ref location, value);
//        public static void Store64Relaxed(ref long location, long value) => Volatile.Write(ref location, value);

//        // --------------------------------------------------------------------------------------------
//        // Exchange
//        // --------------------------------------------------------------------------------------------

//        public static nuint Exchange(ref nuint location, nuint value) =>
//            Interlocked.Exchange(ref location, value);

//        // --------------------------------------------------------------------------------------------
//        // Math Operations (Fetch-Add, Fetch-Sub, And, Or)
//        // C11 fetch_add returnerar det *gamla* värdet. Interlocked.Add returnerar det *nya* värdet.
//        // Interlocked.And/Or (infört i .NET 5) returnerar det gamla värdet (vilket matchar C11).
//        // --------------------------------------------------------------------------------------------

//        public static nint Add(ref nint location, nint value)
//        {
//            // För att få det GAMLA värdet drar vi bort värdet vi just lade till
//            return Interlocked.Add(ref location, value) - value;
//        }

//        public static nint Sub(ref nint location, nint sub)
//        {
//            return Interlocked.Add(ref location, -sub) + sub;
//        }

//        public static long Add64(ref long location, long value)
//        {
//            return Interlocked.Add(ref location, value) - value;
//        }

//        public static nuint And(ref nuint location, nuint value) => Interlocked.And(ref location, value);
//        public static nuint Or(ref nuint location, nuint value) => Interlocked.Or(ref location, value);

//        public static nint Increment(ref nint location) => Interlocked.Increment(ref location) - 1;
//        public static nint Decrement(ref nint location) => Interlocked.Decrement(ref location) + 1;

//        // Speciella metoder för mi_atomic_maxi64
//        public static void Max64(ref long location, long x)
//        {
//            long current = Volatile.Read(ref location);
//            while (current < x && !CasStrong64(ref location, ref current, x))
//            {
//                // Snurra tills värdet är minst x, eller vi lyckas byta
//            }
//        }

//        // --------------------------------------------------------------------------------------------
//        // Yield
//        // --------------------------------------------------------------------------------------------

//        public static void Yield()
//        {
//            // Detta motsvarar hardware pause / _mm_pause / YieldProcessor
//            Thread.SpinWait(1);
//        }

//        // --------------------------------------------------------------------------------------------
//        // Once
//        // --------------------------------------------------------------------------------------------

//        public static bool Once(ref nuint onceLocation)
//        {
//            if (Volatile.Read(ref onceLocation) != 0) return false;
//            nuint expected = 0;
//            return CasStrong(ref onceLocation, ref expected, 1);
//        }
//    }

//    // --------------------------------------------------------------------------------------------
//    // Lås (Locks)
//    // En lättviktig in-process spinlock som motsvarar "poor man's locks" eller mimallocs fallback.
//    // För mer avancerade scenarier rekommenderas System.Threading.SpinLock eller vanligt lock().
//    // --------------------------------------------------------------------------------------------

//    public struct MiLock
//    {
//        // 0 = olåst, 1 = låst
//        private int _locked;

//        public bool TryAcquire()
//        {
//            return Interlocked.CompareExchange(ref _locked, 1, 0) == 0;
//        }

//        public void Acquire()
//        {
//            SpinWait spin = new SpinWait();
//            while (Interlocked.CompareExchange(ref _locked, 1, 0) != 0)
//            {
//                spin.SpinOnce();
//            }
//        }

//        public void Release()
//        {
//            Volatile.Write(ref _locked, 0);
//        }
//    }
//}
