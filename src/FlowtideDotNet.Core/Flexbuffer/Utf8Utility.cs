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

#define TARGET_64BIT
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;



namespace FlowtideDotNet.Core.Flexbuffer
{
    // Code taken from https://github.com/dotnet/runtime/blob/8c21eadb558028181919be879bf125603546ff68/src/libraries/System.Private.CoreLib/src/System/Text/Unicode/Utf8Utility.cs
    // Modified to handle compareTo instead of equals
    internal static class Utf8Utility
    {

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int CompareToOrdinalIgnoreCaseUtf8(in this ReadOnlySpan<byte> span, in ReadOnlySpan<byte> value)
        {
            // For UTF-8 ist is possible for two spans of different byte length
            // to compare as equal under an OrdinalIgnoreCase comparison.
            if ((span.Length | value.Length) == 0)  // span.Length == value.Length == 0
            {
                return 0;
            }

            return EqualsIgnoreCaseUtf8(ref MemoryMarshal.GetReference(span), span.Length, ref MemoryMarshal.GetReference(value), value.Length);
        }


        /// <summary>
        /// Returns true iff the UInt32 represents four ASCII UTF-8 characters in machine endianness.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool AllBytesInUInt32AreAscii(uint value) => (value & ~0x7F7F_7F7Fu) == 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool AllBytesInUInt64AreAscii(ulong value) => (value & ~0x7F7F_7F7F_7F7F_7F7Ful) == 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int EqualsIgnoreCaseUtf8(ref byte charA, int lengthA, ref byte charB, int lengthB)
        {
            return EqualsIgnoreCaseUtf8_Scalar(ref charA, lengthA, ref charB, lengthB);
            //if (!Vector128.IsHardwareAccelerated || (lengthA < Vector128<byte>.Count) || (lengthB < Vector128<byte>.Count))
            //{
            //    return EqualsIgnoreCaseUtf8_Scalar(ref charA, lengthA, ref charB, lengthB);
            //}

            //return EqualsIgnoreCaseUtf8_Vector128(ref charA, lengthA, ref charB, lengthB);
        }

        internal static int EqualsIgnoreCaseUtf8_Scalar(ref byte charA, int lengthA, ref byte charB, int lengthB)
        {
            IntPtr byteOffset = IntPtr.Zero;

            int length = Math.Min(lengthA, lengthB);
            int range = length;

#if TARGET_64BIT
            ulong valueAu64 = 0;
            ulong valueBu64 = 0;

            // Read 8 chars (64 bits) at a time from each string
            while ((uint)length >= 8)
            {
                valueAu64 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref charA, byteOffset));
                valueBu64 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.AddByteOffset(ref charB, byteOffset));

                // A 32-bit test - even with the bit-twiddling here - is more efficient than a 64-bit test.
                ulong temp = valueAu64 | valueBu64;

                if (!Utf8Utility.AllBytesInUInt32AreAscii((uint)temp | (uint)(temp >> 32)))
                {
                    // one of the inputs contains non-ASCII data
                    goto NonAscii64;
                }

                // Generally, the caller has likely performed a first-pass check that the input strings
                // are likely equal. Consider a dictionary which computes the hash code of its key before
                // performing a proper deep equality check of the string contents. We want to optimize for
                // the case where the equality check is likely to succeed, which means that we want to avoid
                // branching within this loop unless we're about to exit the loop, either due to failure or
                // due to us running out of input data.

                var compareToVal = Utf8Utility.UInt64OrdinalIgnoreCaseAscii(valueAu64, valueBu64);
                if (compareToVal != 0)
                {
                    if (compareToVal > 0)
                    {
                        return 1;
                    }
                    else
                    {
                        return -1;
                    }
                    //return compareToVal;
                }

                byteOffset += 8;
                length -= 8;
            }
#endif

            uint valueAu32 = 0;
            uint valueBu32 = 0;

            // Read 4 chars (32 bits) at a time from each string
#if TARGET_64BIT
            if ((uint)length >= 4)
#else
            while ((uint)length >= 4)
#endif
            {
                valueAu32 = Unsafe.ReadUnaligned<uint>(ref Unsafe.AddByteOffset(ref charA, byteOffset));
                valueBu32 = Unsafe.ReadUnaligned<uint>(ref Unsafe.AddByteOffset(ref charB, byteOffset));

                if (!Utf8Utility.AllBytesInUInt32AreAscii(valueAu32 | valueBu32))
                {
                    // one of the inputs contains non-ASCII data
                    goto NonAscii32;
                }

                // Generally, the caller has likely performed a first-pass check that the input strings
                // are likely equal. Consider a dictionary which computes the hash code of its key before
                // performing a proper deep equality check of the string contents. We want to optimize for
                // the case where the equality check is likely to succeed, which means that we want to avoid
                // branching within this loop unless we're about to exit the loop, either due to failure or
                // due to us running out of input data.

                var compareVal = UInt32OrdinalIgnoreCaseAscii(valueAu32, valueBu32);
                if (compareVal != 0)
                {
                    return compareVal;
                }

                byteOffset += 4;
                length -= 4;
            }

            if (length != 0)
            {
                // We have 1, 2, or 3 bytes remaining. We can't do anything fancy
                // like backtracking since we could have only had 1-3 bytes. So,
                // instead we'll do 1 or 2 reads to get all 3 bytes. Endianness
                // doesn't matter here since we only compare if all bytes are ascii
                // and the ordering will be consistent between the two comparisons

                if (length == 3)
                {
                    valueAu32 = Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref charA, byteOffset));
                    valueBu32 = Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref charB, byteOffset));

                    byteOffset += 2;

                    valueAu32 |= (uint)(Unsafe.AddByteOffset(ref charA, byteOffset) << 16);
                    valueBu32 |= (uint)(Unsafe.AddByteOffset(ref charB, byteOffset) << 16);
                }
                else if (length == 2)
                {
                    valueAu32 = Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref charA, byteOffset));
                    valueBu32 = Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref charB, byteOffset));
                }
                else
                {
                    Debug.Assert(length == 1);

                    valueAu32 = Unsafe.AddByteOffset(ref charA, byteOffset);
                    valueBu32 = Unsafe.AddByteOffset(ref charB, byteOffset);
                }

                if (!Utf8Utility.AllBytesInUInt32AreAscii(valueAu32 | valueBu32))
                {
                    // one of the inputs contains non-ASCII data
                    goto NonAscii32;
                }

                if (lengthA != lengthB)
                {
                    // Failure if we reached the end of one, but not both sequences
                    return lengthA - lengthB;
                }

                if (valueAu32 == valueBu32)
                {
                    // exact match
                    return 0;
                }

                return Utf8Utility.UInt32OrdinalIgnoreCaseAscii(valueAu32, valueBu32);
            }

            Debug.Assert(length == 0);
            return lengthA - lengthB;

        NonAscii32:
            // Both values have to be non-ASCII to use the slow fallback, in case if one of them is not we return false
            //if (Utf8Utility.AllBytesInUInt32AreAscii(valueAu32) || Utf8Utility.AllBytesInUInt32AreAscii(valueBu32))
            //{
            //    return false;
            //}
            goto NonAscii;

#if TARGET_64BIT
        NonAscii64:
            // Both values have to be non-ASCII to use the slow fallback, in case if one of them is not we return false
            //if (Utf8Utility.AllBytesInUInt64AreAscii(valueAu64) || Utf8Utility.AllBytesInUInt64AreAscii(valueBu64))
            //{
            //    return false;
            //}
            goto NonAscii;
#endif
        NonAscii:
            range -= length;

            // The non-ASCII case is factored out into its own helper method so that the JIT
            // doesn't need to emit a complex prolog for its caller (this method).
            return CompareToStringIgnoreCaseUtf8(ref Unsafe.AddByteOffset(ref charA, byteOffset), lengthA - range, ref Unsafe.AddByteOffset(ref charB, byteOffset), lengthB - range);
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int UInt32OrdinalIgnoreCaseAscii(uint valueA, uint valueB)
        {
            // Not currently intrinsified in mono interpreter, the UTF16 version is
            // ASSUMPTION: Caller has validated that input values are ASCII.
            Debug.Assert(AllBytesInUInt32AreAscii(valueA));
            Debug.Assert(AllBytesInUInt32AreAscii(valueB));

            // The logic here is very simple and is doing SIMD Within A Register (SWAR)
            //
            // First we want to create a mask finding the upper-case ASCII characters
            //
            // To do that, we can take the above presumption that all characters are ASCII
            // and therefore between 0x00 and 0x7F, inclusive. This means that `0x80 + char`
            // will never overflow and will at most produce 0xFF.
            //
            // Given that, we can check if a byte is greater than a value by adding it to
            // 0x80 and then subtracting the constant we're comparing against. So, for example,
            // if we want to find all characters greater than 'A' we do `value + 0x80 - 'A'`.
            //
            // Given that 'A' is 0x41, we end up with `0x41 + 0x80 == 0xC1` then we subtract 'A'
            // giving us `0xC1 - 0x41 == 0x80` and up to `0xBE` for 'DEL' (0x7F). This means that
            // any character greater than or equal to 'A' will have the most significant bit set.
            //
            // This can itself be simplified down to `val + (0x80 - 'A')` or `val + 0x3F`
            //
            // We also want to find the characters less than or equal to 'Z' as well. This follows
            // the same general principle but relies on finding the inverse instead. That is, we
            // want to find all characters greater than or equal to ('Z' + 1) and then inverse it.
            //
            // To confirm this, lets look at 'Z' which has the value of '0x5A'. So we first do
            // `0x5A + 0x80 == 0xDA`, then we subtract `[' (0x5B) giving us `0xDA - 0x5B == 0x80`.
            // This means that any character greater than 'Z' will now have the most significant bit set.
            //
            // It then follows that taking the ones complement will give us a mask representing the bytes
            // which are less than or equal to 'Z' since `!(val >= 0x5B) == (val <= 0x5A)`
            //
            // This then gives us that `('A' <= val) && (val <= 'Z')` is representable as
            // `(val + 0x3F) & ~(val + 0x25)`
            //
            // However, since a `val` cannot be simultaneously less than 'A' and greater than 'Z' we
            // are able to simplify this further to being just `(val + 0x3F) ^ (val + 0x25)`
            //
            // We then want to mask off the excess bits that aren't important to the mask and right
            // shift by two. This gives us `0x20` for a byte which is an upper-case ASCII character
            // and `0x00` otherwise.
            //
            // We now have a super efficient implementation that does a correct comparison in
            // 12 instructions and with zero branching.

            uint letterMaskA = (((valueA + 0x3F3F3F3F) ^ (valueA + 0x25252525)) & 0x80808080) >> 2;
            uint letterMaskB = (((valueB + 0x3F3F3F3F) ^ (valueB + 0x25252525)) & 0x80808080) >> 2;

            int maskedLeft = (int)(valueA | letterMaskA);
            int maskedRight = (int)(valueB | letterMaskB);

            return maskedLeft - maskedRight;
            //if (maskedLeft < maskedRight)
            //{
            //    return -1;
            //}
            //if (maskedLeft > maskedRight)
            //{
            //    return 1;
            //}
            //return 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long UInt64OrdinalIgnoreCaseAscii(ulong valueA, ulong valueB)
        {
            // Not currently intrinsified in mono interpreter, the UTF16 version is
            // ASSUMPTION: Caller has validated that input values are ASCII.
            Debug.Assert(AllBytesInUInt64AreAscii(valueA));
            Debug.Assert(AllBytesInUInt64AreAscii(valueB));

            // Duplicate of logic in UInt32OrdinalIgnoreCaseAscii, but using 64-bit consts.
            // See comments in that method for more info.

            ulong letterMaskA = (((valueA + 0x3F3F3F3F3F3F3F3F) ^ (valueA + 0x2525252525252525)) & 0x8080808080808080) >> 2;
            ulong letterMaskB = (((valueB + 0x3F3F3F3F3F3F3F3F) ^ (valueB + 0x2525252525252525)) & 0x8080808080808080) >> 2;

            var maskedLeft = (long)(valueA | letterMaskA);
            var maskedRight = (long)(valueB | letterMaskB);

            return maskedLeft - maskedRight;
            //if (maskedLeft < maskedRight)
            //{
            //    return -1;
            //}
            //if (maskedLeft > maskedRight)
            //{
            //    return 1;
            //}
            //return 0;
        }

        internal static int CompareToStringIgnoreCaseUtf8(ref byte strA, int lengthA, ref byte strB, int lengthB)
        {
            // NOTE: Two UTF-8 inputs of different length might compare as equal under
            // the OrdinalIgnoreCase comparer. This is distinct from UTF-16, where the
            // inputs being different length will mean that they can never compare as
            // equal under an OrdinalIgnoreCase comparer.

            int length = Math.Min(lengthA, lengthB);
            int range = length;

            ref byte charA = ref strA;
            ref byte charB = ref strB;

            const byte maxChar = 0x7F;

            while ((length != 0) && (charA <= maxChar) && (charB <= maxChar))
            {
                // Ordinal equals or lowercase equals if the result ends up in the a-z range
                if (charA == charB ||
                    ((charA | 0x20) == (charB | 0x20) && char.IsAsciiLetter((char)charA)))
                {
                    length--;
                    charA = ref Unsafe.Add(ref charA, 1);
                    charB = ref Unsafe.Add(ref charB, 1);
                }
                else
                {
                    if (char.IsAsciiLetter((char)charA) && char.IsAsciiLetter((char)charB))
                    {
                        return (charA | 0x20) - (charB | 0x20);
                    }
                    return charA - charB;
                }
            }

            if (length == 0)
            {
                // Success if we reached the end of both sequences
                return lengthA - lengthB;
            }

            range -= length;
            return EqualsStringIgnoreCaseNonAsciiUtf8(ref charA, lengthA - range, ref charB, lengthB - range);
        }

        internal static int EqualsStringIgnoreCaseNonAsciiUtf8(ref byte strA, int lengthA, ref byte strB, int lengthB)
        {
            // NLS/ICU doesn't provide native UTF-8 support so we need to do our own corresponding ordinal comparison

            ReadOnlySpan<byte> spanA = MemoryMarshal.CreateReadOnlySpan(ref strA, lengthA);
            ReadOnlySpan<byte> spanB = MemoryMarshal.CreateReadOnlySpan(ref strB, lengthB);

            do
            {
                OperationStatus statusA = Rune.DecodeFromUtf8(spanA, out Rune runeA, out int bytesConsumedA);
                OperationStatus statusB = Rune.DecodeFromUtf8(spanB, out Rune runeB, out int bytesConsumedB);

                if (statusA != statusB)
                {
                    // OperationStatus don't match; fail immediately
                    throw new Exception("Can this ever happen? Please notify if this exception is thrown");
                }

                if (statusA == OperationStatus.Done)
                {
                    var compareToVal = Rune.ToUpperInvariant(runeA).CompareTo(runeB);
                    if (compareToVal != 0)
                    {
                        return compareToVal;
                    }
                    //if (Rune.ToUpperInvariant(runeA) != Rune.ToUpperInvariant(runeB))
                    //{
                    //    // Runes don't match when ignoring case; fail immediately
                    //    return false;
                    //}
                }
                else
                {
                    var compareResult = spanA.Slice(0, bytesConsumedA).SequenceCompareTo(spanB.Slice(0, bytesConsumedB));

                    if (compareResult != 0)
                    {
                        return compareResult;
                    }
                    // OperationStatus match, but bytesConsumed or the sequence of bytes consumed do not; fail immediately
                    //return false;
                }

                // The current runes or invalid byte sequences matched, slice and continue.
                // We'll exit the loop when the entirety of both spans have been processed.
                //
                // In the scenario where one buffer is empty before the other, we'll end up
                // with that span returning OperationStatus.NeedsMoreData and bytesConsumed=0
                // while the other span will return a different OperationStatus or different
                // bytesConsumed and thus fail the operation.

                spanA = spanA.Slice(bytesConsumedA);
                spanB = spanB.Slice(bytesConsumedB);
            }
            while ((spanA.Length | spanB.Length) != 0);

            return 0;
        }

    }
}
