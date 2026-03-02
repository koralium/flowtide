using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using static mimalloctests.MiMalloc;

namespace mimalloctests
{
    public unsafe partial class MiMalloc
    {
        /* ----------------------------------------------------------------------------
            We use our own PRNG to keep predictable performance of random number generation
            and to avoid implementations that use a lock. We only use the OS provided
            random source to initialize the initial seeds. Since we do not need ultimate
            performance but we do rely on the security (for secret cookies in secure mode)
            we use a cryptographically secure generator (chacha20).
            -----------------------------------------------------------------------------*/

        // perhaps use 12 for better performance?
        private const nuint MI_CHACHA_ROUNDS = 20;

        /* ----------------------------------------------------------------------------
        Chacha20 implementation as the original algorithm with a 64-bit nonce
        and counter: https://en.wikipedia.org/wiki/Salsa20
        The input matrix has sixteen 32-bit values:
        Position  0 to  3: constant key
        Position  4 to 11: the key
        Position 12 to 13: the counter.
        Position 14 to 15: the nonce.

        The implementation uses regular C code which compiles very well on modern compilers.
        (gcc x64 has no register spills, and clang 6+ uses SSE instructions)
        -----------------------------------------------------------------------------*/

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static uint rotl(uint x, uint shift) => BitOperations.RotateLeft(x, (int)shift);

        private static void qround(uint* x, nuint a, nuint b, nuint c, nuint d)
        {
            unchecked
            {
                x[a] += x[b];
                x[d] = rotl(x[d] ^ x[a], 16);

                x[c] += x[d];
                x[b] = rotl(x[b] ^ x[c], 12);

                x[a] += x[b];
                x[d] = rotl(x[d] ^ x[a], 8);

                x[c] += x[d];
                x[b] = rotl(x[b] ^ x[c], 7);
            }
        }

        private static void chacha_block(ref mi_random_ctx_t ctx)
        {
            // scramble into `x`
            uint* x = stackalloc uint[16];

            for (nuint i = 0; i < 16; i++)
            {
                x[i] = ctx.input[i];
            }
            for (nuint i = 0; i < MI_CHACHA_ROUNDS; i += 2)
            {
                qround(x, 0, 4, 8, 12);
                qround(x, 1, 5, 9, 13);
                qround(x, 2, 6, 10, 14);
                qround(x, 3, 7, 11, 15);
                qround(x, 0, 5, 10, 15);
                qround(x, 1, 6, 11, 12);
                qround(x, 2, 7, 8, 13);
                qround(x, 3, 4, 9, 14);
            }

            // add scrambled data to the initial state
            for (nuint i = 0; i < 16; i++)
            {
                ctx.output[i] = unchecked(x[i] + ctx.input[i]);
            }

            ctx.output_available = 16;

            // increment the counter for the next round
            ctx.input[12] += 1;

            if (ctx.input[12] == 0)
            {
                ctx.input[13] += 1;

                if (ctx.input[13] == 0)
                {
                    // and keep increasing into the nonce
                    ctx.input[14] += 1;
                }
            }
        }


        private static uint chacha_next32(ref mi_random_ctx_t ctx)
        {
            if (ctx.output_available <= 0)
            {
                chacha_block(ref ctx);
                ctx.output_available = 16; // (assign again to suppress static analysis warning)
            }

            uint x = ctx.output[16 - ctx.output_available];

            ctx.output[16 - ctx.output_available] = 0; // reset once the data is handed out
            ctx.output_available--;

            return x;
        }

        private static uint read32(ReadOnlySpan<byte> p, nuint idx32)
        {
            ReadOnlySpan<byte> span = p.Slice((int)(4 * idx32), 4);
            return BinaryPrimitives.ReadUInt32LittleEndian(span);
        }

        // "expand 32-byte k"
        private static ReadOnlySpan<byte> sigma => [0x65, 0x78, 0x70, 0x61, 0x6E, 0x64, 0x20, 0x33, 0x32, 0x2D, 0x62, 0x79, 0x74, 0x65, 0x20, 0x6B, 0x00];

        private static void chacha_init(out mi_random_ctx_t ctx, ReadOnlySpan<byte> key, ulong nonce)
        {
            // since we only use chacha for randomness (and not encryption) we
            // do not _need_ to read 32-bit values as little endian but we do anyways
            // just for being compatible :-)

            ctx = default;

            for (nuint i = 0; i < 4; i++)
            {
                ctx.input[i] = read32(sigma, i);
            }

            for (nuint i = 0; i < 8; i++)
            {
                ctx.input[i + 4] = read32(key, i);
            }

            ctx.input[12] = 0;
            ctx.input[13] = 0;
            ctx.input[14] = unchecked((uint)nonce);
            ctx.input[15] = (uint)(nonce >> 32);
        }

        private static void chacha_split(in mi_random_ctx_t ctx, ulong nonce, out mi_random_ctx_t ctx_new)
        {
            ctx_new = default;
            Unsafe.CopyBlock(ref Unsafe.As<uint, byte>(ref ctx_new.input[0]), ref Unsafe.As<uint, byte>(ref Unsafe.AsRef(in ctx.input[0])), 16);

            ctx_new.input[12] = 0;
            ctx_new.input[13] = 0;
            ctx_new.input[14] = unchecked((uint)nonce);
            ctx_new.input[15] = (uint)(nonce >> 32);

            mi_assert_internal((MI_DEBUG > 1) && ((ctx.input[14] != ctx_new.input[14]) || (ctx.input[15] != ctx_new.input[15]))); // do not reuse nonces!
            chacha_block(ref ctx_new);
        }


        /* ----------------------------------------------------------------------------
        Random interface
        -----------------------------------------------------------------------------*/

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool mi_random_is_initialized(in mi_random_ctx_t ctx)
        {
            mi_assert_internal((MI_DEBUG > 1) && (MI_DEBUG > 1));
            return ctx.input[0] != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void _mi_random_split(in mi_random_ctx_t ctx, out mi_random_ctx_t ctx_new)
        {
            Unsafe.SkipInit(out ctx_new);

            mi_assert_internal((MI_DEBUG > 1) && mi_random_is_initialized(ctx));
            mi_assert_internal((MI_DEBUG > 1) && (!Unsafe.AreSame(ref Unsafe.AsRef(in ctx), ref ctx_new)));

            chacha_split(in ctx, nonce: (nuint)Unsafe.AsPointer(ref ctx_new), out ctx_new);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint _mi_random_next(ref mi_random_ctx_t ctx)
        {
            mi_assert_internal((MI_DEBUG > 1) && mi_random_is_initialized(in ctx));

            if (Environment.Is64BitProcess)
            {
                return ((nuint)chacha_next32(ref ctx) << 32) | chacha_next32(ref ctx);
            }
            else
            {
                return chacha_next32(ref ctx);
            }
        }

        /* ----------------------------------------------------------------------------
        To initialize a fresh random context we rely on .NET
        -----------------------------------------------------------------------------*/

        // The following members have not been ported as they aren't needed for .NET:
        //  * void os_random_buf(void*, size_t)
        //  * uintptr_t _os_random_weak(uintptr_t)

        private static void _mi_random_init(out mi_random_ctx_t ctx)
        {
            Unsafe.SkipInit(out ctx);

            Span<byte> key = stackalloc byte[32];
            RandomNumberGenerator.Fill(key);

            chacha_init(out ctx, key, nonce: (nuint)Unsafe.AsPointer(ref ctx));
        }

    }
}
