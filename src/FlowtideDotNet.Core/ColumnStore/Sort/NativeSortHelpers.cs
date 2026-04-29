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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool GetBit(void* ptr, int index)
        {
            var wordIndex = index >> 5;
            int bitIndex = 1 << (index & 31);
            return (((int*)ptr)[wordIndex] & bitIndex) != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareBits(void* ptr, int x, int y)
        {
            int* intPtr = (int*)ptr;
            int bitX = (intPtr[x >> 5] >> (x & 31)) & 1;
            int bitY = (intPtr[y >> 5] >> (y & 31)) & 1;
            return bitX - bitY;
        }

        public static Expression CallCompareBits(Expression selfComparePointers, Expression x, Expression y)
        {
            var methodInfo = typeof(NativeSortHelpers).GetMethod(nameof(CompareBits), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
            if (methodInfo == null)
            {
                throw new InvalidOperationException("Method not found: " + nameof(CompareBits));
            }
            var validityPointerField = typeof(SelfComparePointers).GetField("validityPointer");

            if (validityPointerField == null)
            {
                throw new InvalidOperationException("Field not found: validityPointer");

            }
            var getPtrExpression = Expression.Field(selfComparePointers, validityPointerField);

            return Expression.Call(methodInfo, getPtrExpression, x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
    }
}
