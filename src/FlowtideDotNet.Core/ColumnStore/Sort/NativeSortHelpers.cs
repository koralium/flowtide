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

using FlowtideDotNet.Core.ColumnStore.DataValues;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Sort
{
    internal unsafe static class NativeSortHelpers
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static bool GetBit(void* ptr, int index)
        {
            var wordIndex = index >> 5;
            int bitIndex = 1 << (index & 31);
            return (((int*)ptr)[wordIndex] & bitIndex) != 0;
        }

        /// <summary>
        /// Compare validity bits, returns 2 if both are valid to make easy branching.
        /// If either both are invalid or one is valid and the other not, it returns -1, 0, 1.
        /// </summary>
        /// <param name="ptr"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static int CompareValidityBits(void* ptr, int x, int y)
        {
            int* intPtr = (int*)ptr;

            int maskX = ~(x >> 31);
            int clampX = x & maskX;
            int bitX = ((intPtr[clampX >> 5] >> (clampX & 31)) & 1) & maskX;

            int maskY = ~(y >> 31);
            int clampY = y & maskY;
            int bitY = ((intPtr[clampY >> 5] >> (clampY & 31)) & 1) & maskY;

            return (bitX - bitY) + ((bitX & bitY) << 1);
        }

        public static Expression CallCompareValidityBits(Expression selfComparePointers, Expression x, Expression y)
        {
            var methodInfo = typeof(NativeSortHelpers).GetMethod(nameof(CompareValidityBits), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
            if (methodInfo == null)
            {
                throw new InvalidOperationException("Method not found: " + nameof(CompareValidityBits));
            }
            var validityPointerField = typeof(SelfComparePointers).GetField("validityPointer");

            if (validityPointerField == null)
            {
                throw new InvalidOperationException("Field not found: validityPointer");

            }
            var getPtrExpression = Expression.Field(selfComparePointers, validityPointerField);

            return Expression.Call(methodInfo, getPtrExpression, x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static int GetInt32(void* ptr, int index)
        {
            return ((int*)ptr)[index];
        }

        public static Expression CallGetColumnOffset(Expression selfComparePointers, Expression index)
        {
            var methodInfo = typeof(NativeSortHelpers).GetMethod(nameof(GetInt32), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
            if (methodInfo == null)
            {
                throw new InvalidOperationException("Method not found: " + nameof(GetInt32));
            }
            var dataPointerField = typeof(SelfComparePointers).GetField("columnOffsetsPointer");
            if (dataPointerField == null)
            {
                throw new InvalidOperationException("Field not found: columnOffsetsPointer");
            }
            var getPtrExpression = Expression.Field(selfComparePointers, dataPointerField);
            return Expression.Call(methodInfo, getPtrExpression, index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static int CompareDouble(void* ptr, int x, int y)
        {
            var doublePtr = (double*)ptr;
            double valueX = doublePtr[x];
            double valueY = doublePtr[y];
            return valueX.CompareTo(valueY);
        }

        public static Expression CallCompareDouble(Expression selfComparePointers, Expression x, Expression y)
        {
            var methodInfo = typeof(NativeSortHelpers).GetMethod(nameof(CompareDouble), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
            if (methodInfo == null)
            {
                throw new InvalidOperationException("Method not found: " + nameof(CompareDouble));
            }
            var doublePointerField = typeof(SelfComparePointers).GetField("dataPointer");
            if (doublePointerField == null)
            {
                throw new InvalidOperationException("Field not found: dataPointer");
            }
            var getPtrExpression = Expression.Field(selfComparePointers, doublePointerField);
            return Expression.Call(methodInfo, getPtrExpression, x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static int CompareInt32(void* ptr, int x, int y)
        {
            var intPtr = (int*)ptr;
            int valueX = intPtr[x];
            int valueY = intPtr[y];
            return valueX.CompareTo(valueY);
        }

        public static Expression CallCompareInt32(Expression selfComparePointers, Expression x, Expression y)
        {
            var methodInfo = typeof(NativeSortHelpers).GetMethod(nameof(CompareInt32), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
            if (methodInfo == null)
            {
                throw new InvalidOperationException("Method not found: " + nameof(CompareInt32));
            }
            var intPointerField = typeof(SelfComparePointers).GetField("dataPointer");
            if (intPointerField == null)
            {
                throw new InvalidOperationException("Field not found: dataPointer");
            }
            var getPtrExpression = Expression.Field(selfComparePointers, intPointerField);
            return Expression.Call(methodInfo, getPtrExpression, x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static int CompareInt8(void* ptr, int x, int y)
        {
            var intPtr = (sbyte*)ptr;
            sbyte valueX = intPtr[x];
            sbyte valueY = intPtr[y];
            return valueX.CompareTo(valueY);
        }

        public static Expression CallCompareInt8(Expression selfComparePointers, Expression x, Expression y)
        {
            var methodInfo = typeof(NativeSortHelpers).GetMethod(nameof(CompareInt8), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
            if (methodInfo == null)
            {
                throw new InvalidOperationException("Method not found: " + nameof(CompareInt8));
            }
            var intPointerField = typeof(SelfComparePointers).GetField("dataPointer");
            if (intPointerField == null)
            {
                throw new InvalidOperationException("Field not found: dataPointer");
            }
            var getPtrExpression = Expression.Field(selfComparePointers, intPointerField);
            return Expression.Call(methodInfo, getPtrExpression, x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static int CompareInt16(void* ptr, int x, int y)
        {
            var intPtr = (short*)ptr;
            short valueX = intPtr[x];
            short valueY = intPtr[y];
            return valueX.CompareTo(valueY);
        }

        public static Expression CallCompareInt16(Expression selfComparePointers, Expression x, Expression y)
        {
            var methodInfo = typeof(NativeSortHelpers).GetMethod(nameof(CompareInt16), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
            if (methodInfo == null)
            {
                throw new InvalidOperationException("Method not found: " + nameof(CompareInt16));
            }
            var intPointerField = typeof(SelfComparePointers).GetField("dataPointer");
            if (intPointerField == null)
            {
                throw new InvalidOperationException("Field not found: dataPointer");
            }
            var getPtrExpression = Expression.Field(selfComparePointers, intPointerField);
            return Expression.Call(methodInfo, getPtrExpression, x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static int CompareInt64(void* ptr, int x, int y)
        {
            var intPtr = (long*)ptr;
            long valueX = intPtr[x];
            long valueY = intPtr[y];
            return valueX.CompareTo(valueY);
        }

        public static Expression CallCompareInt64(Expression selfComparePointers, Expression x, Expression y)
        {
            var methodInfo = typeof(NativeSortHelpers).GetMethod(nameof(CompareInt64), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
            if (methodInfo == null)
            {
                throw new InvalidOperationException("Method not found: " + nameof(CompareInt64));
            }
            var intPointerField = typeof(SelfComparePointers).GetField("dataPointer");
            if (intPointerField == null)
            {
                throw new InvalidOperationException("Field not found: dataPointer");
            }
            var getPtrExpression = Expression.Field(selfComparePointers, intPointerField);
            return Expression.Call(methodInfo, getPtrExpression, x, y);
        }

        /// <summary>
        /// This method compares offsets for column with offset, -1 is same as null
        /// It returns nr 2 if both are above or equal to 0 as a special case.
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static int CompareOffsets(int x, int y)
        {
            int sx = x >> 31; // 0 if valid (>=0), -1 if null (<0)
            int sy = y >> 31; // 0 if valid (>=0), -1 if null (<0)

            return (sx - sy) + (((sx | sy) + 1) << 1);
        }

        public static Expression CallCompareOffsets(Expression x, Expression y)
        {
            var methodInfo = typeof(NativeSortHelpers).GetMethod(nameof(CompareOffsets), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
            if (methodInfo == null)
            {
                throw new InvalidOperationException("Method not found: " + nameof(CompareOffsets));
            }
            return Expression.Call(methodInfo, x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static int CompareBinary(void* dataPointer, void* offsetPointer, int x, int y)
        {
            if (x == y) return 0;

            int* offsets = (int*)offsetPointer;
            byte* data = (byte*)dataPointer;

            int startX = offsets[x];
            int endX = offsets[x + 1];
            int lengthX = endX - startX;

            int startY = offsets[y];
            int endY = offsets[y + 1];
            int lengthY = endY - startY;

            var spanX = new ReadOnlySpan<byte>(data + startX, lengthX);
            var spanY = new ReadOnlySpan<byte>(data + startY, lengthY);

            return spanX.SequenceCompareTo(spanY);
        }

        public static Expression CallCompareBinary(Expression selfComparePointers, Expression x, Expression y)
        {
            var methodInfo = typeof(NativeSortHelpers).GetMethod(nameof(CompareBinary), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
            if (methodInfo == null)
            {
                throw new InvalidOperationException("Method not found: " + nameof(CompareBinary));
            }
            var dataPointerField = typeof(SelfComparePointers).GetField("dataPointer");
            var offsetPointerField = typeof(SelfComparePointers).GetField("secondaryPointer");
            if (dataPointerField == null || offsetPointerField == null)
            {
                throw new InvalidOperationException("Field not found: dataPointer or secondaryPointer");
            }
            var dataPtrExpression = Expression.Field(selfComparePointers, dataPointerField);
            var offsetPtrExpression = Expression.Field(selfComparePointers, offsetPointerField);
            return Expression.Call(methodInfo, dataPtrExpression, offsetPtrExpression, x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static int CompareBools(void* ptr, int x, int y)
        {
            int* intPtr = (int*)ptr;

            int maskX = ~(x >> 31);
            int clampX = x & maskX;
            int bitX = ((intPtr[clampX >> 5] >> (clampX & 31)) & 1) & maskX;

            int maskY = ~(y >> 31);
            int clampY = y & maskY;
            int bitY = ((intPtr[clampY >> 5] >> (clampY & 31)) & 1) & maskY;

            return bitX - bitY;
        }

        public static Expression CallCompareBools(Expression selfComparePointers, Expression x, Expression y)
        {
            var methodInfo = typeof(NativeSortHelpers).GetMethod(nameof(CompareBools), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
            if (methodInfo == null)
            {
                throw new InvalidOperationException("Method not found: " + nameof(CompareBools));
            }
            var dataPointerField = typeof(SelfComparePointers).GetField("dataPointer");
            if (dataPointerField == null)
            {
                throw new InvalidOperationException("Field not found: dataPointer");
            }
            var getPtrExpression = Expression.Field(selfComparePointers, dataPointerField);
            return Expression.Call(methodInfo, getPtrExpression, x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static int CompareDecimal128(void* ptr, int x, int y)
        {
            var decimalPtr = (decimal*)ptr;
            decimal valueX = decimalPtr[x];
            decimal valueY = decimalPtr[y];
            return valueX.CompareTo(valueY);
        }

        public static Expression CallCompareDecimal128(Expression selfComparePointers, Expression x, Expression y)
        {
            var methodInfo = typeof(NativeSortHelpers).GetMethod(nameof(CompareDecimal128), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
            if (methodInfo == null)
            {
                throw new InvalidOperationException("Method not found: " + nameof(CompareDecimal128));
            }
            var decimalPointerField = typeof(SelfComparePointers).GetField("dataPointer");
            if (decimalPointerField == null)
            {
                throw new InvalidOperationException("Field not found: dataPointer");
            }
            var getPtrExpression = Expression.Field(selfComparePointers, decimalPointerField);
            return Expression.Call(methodInfo, getPtrExpression, x, y);
        }

        public static int CompareTimestampTzValues(void* ptr, int x, int y)
        {
            var timestampTzPtr = (TimestampTzValue*)ptr;
            TimestampTzValue valueX = timestampTzPtr[x];
            TimestampTzValue valueY = timestampTzPtr[y];
            return valueX.CompareTo(valueY);
        }

        public static Expression CallCompareTimestampTzValues(Expression selfComparePointers, Expression x, Expression y)
        {
            var methodInfo = typeof(NativeSortHelpers).GetMethod(nameof(CompareTimestampTzValues), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
            if (methodInfo == null)
            {
                throw new InvalidOperationException("Method not found: " + nameof(CompareTimestampTzValues));
            }
            var timestampTzPointerField = typeof(SelfComparePointers).GetField("dataPointer");
            if (timestampTzPointerField == null)
            {
                throw new InvalidOperationException("Field not found: dataPointer");
            }
            var getPtrExpression = Expression.Field(selfComparePointers, timestampTzPointerField);
            return Expression.Call(methodInfo, getPtrExpression, x, y);
        }
    }
}
