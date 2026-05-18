// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

namespace FlowtideDotNet.Nexmark.Internal;

/// <summary>
/// A C# port of java.util.Random.
/// This ensures byte-for-byte exact reproducibility with the original Java NEXMark generator.
/// Java's Random uses a 48-bit Linear Congruential Generator (LCG).
/// </summary>
internal sealed class JavaRandom
{
    private long _seed;
    private const long Multiplier = 0x5DEECE66DL;
    private const long Addend = 0xBL;
    private const long Mask = (1L << 48) - 1;

    public JavaRandom(long seed)
    {
        _seed = (seed ^ Multiplier) & Mask;
    }

    private int NextBits(int bits)
    {
        _seed = (_seed * Multiplier + Addend) & Mask;
        return (int)(_seed >>> (48 - bits));
    }

    public int Next()
    {
        return NextBits(32);
    }

    public int Next(int bound)
    {
        if (bound <= 0)
            throw new ArgumentOutOfRangeException(nameof(bound), "bound must be positive");

        if ((bound & -bound) == bound)  // i.e., bound is a power of 2
            return (int)((bound * (long)NextBits(31)) >> 31);

        int bits, val;
        do
        {
            bits = NextBits(31);
            val = bits % bound;
        } while (bits - val + (bound - 1) < 0);
        return val;
    }

    public double NextDouble()
    {
        return (((long)NextBits(26) << 27) + NextBits(27)) / (double)(1L << 53);
    }

    public bool NextBoolean()
    {
        return NextBits(1) != 0;
    }
}
