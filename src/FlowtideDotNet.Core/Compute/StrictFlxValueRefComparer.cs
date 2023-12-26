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

using FlowtideDotNet.Core.Flexbuffer;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute
{
    internal static class StrictFlxValueRefComparer
    {
        private delegate int CompareFlex(in FlxValueRef a, in FlxValueRef b);

        private static CompareFlex[] compareFunctions = CreateCompareFunctions();

        private static CompareFlex[] CreateCompareFunctions()
        {
            CompareFlex[] functions = new CompareFlex[32];

            functions[(int)FlexBuffers.Type.Null] = CompareNull;
            functions[(int)FlexBuffers.Type.Bool] = CompareBool;
            functions[(int)FlexBuffers.Type.Int] = CompareInt;
            functions[(int)FlexBuffers.Type.String] = CompareString;
            functions[(int)FlexBuffers.Type.Float] = CompareFloat;
            functions[(int)FlexBuffers.Type.Vector] = CompareVector;
            functions[(int)FlexBuffers.Type.Map] = CompareMap;
            functions[(int)FlexBuffers.Type.Key] = CompareKey;
            functions[(int)FlexBuffers.Type.Blob] = CompareBlob;
            functions[(int)FlexBuffers.Type.Decimal] = CompareDecimal;

            return functions;
        }

        private static int CompareNull(in FlxValueRef a, in FlxValueRef b)
        {
            return 0;
        }

        private static int CompareBool(in FlxValueRef a, in FlxValueRef b)
        {
            return a._buffer[a._offset] - b._buffer[b._offset];
        }

        private static int CompareInt(in FlxValueRef a, in FlxValueRef b)
        {
            return a.AsLong.CompareTo(b.AsLong);
        }

        private static int CompareString(in FlxValueRef a, in FlxValueRef b)
        {
            return FlxString.Compare(a.AsFlxString, b.AsFlxString);
        }

        private static int CompareFloat(in FlxValueRef a, in FlxValueRef b)
        {
            return a.AsDouble.CompareTo(b.AsDouble);
        }

        private static int CompareVector(in FlxValueRef a, in FlxValueRef b)
        {
            var avec = a.AsVector;
            var bvec = b.AsVector;

            // First just go after length of vector as a comparison
            var lengthCompare = avec.Length.CompareTo(bvec.Length);

            if (lengthCompare != 0)
            {
                return lengthCompare;
            }

            // Compare each element in order, if one doesnt match return the compare value
            for (int i = 0; i < avec.Length; i++)
            {
                int valCompare = CompareTo(avec[i], bvec[i]);
                if (valCompare != 0)
                {
                    return valCompare;
                }
            }
            return 0;
        }

        private static int CompareMap(in FlxValueRef a, in FlxValueRef b)
        {
            var amap = a.AsMap;
            var bmap = b.AsMap;

            // First just go after length of vector as a comparison
            var lengthCompare = amap.Length.CompareTo(bmap.Length);
            if (lengthCompare != 0)
            {
                return lengthCompare;
            }

            // Compare each key
            for (int i = 0; i < amap.Length; i++)
            {
                int keyCompare = CompareTo(amap.Keys[i], bmap.Keys[i]);
                if (keyCompare != 0)
                {
                    return keyCompare;
                }
            }

            // Compare each element in order, if one doesnt match return the compare value
            for (int i = 0; i < amap.Length; i++)
            {
                int valCompare = CompareTo(amap.Values[i], bmap.Values[i]);
                if (valCompare != 0)
                {
                    return valCompare;
                }
            }
            return 0;
        }

        private static int CompareKey(in FlxValueRef a, in FlxValueRef b)
        {
            return string.Compare(a.AsString, b.AsString);
        }

        private static int CompareBlob(in FlxValueRef a, in FlxValueRef b)
        {
            var ablob = a.AsBlob;
            var bblob = b.AsBlob;
            return ablob.SequenceCompareTo(bblob);
        }

        private static int CompareDecimal(in FlxValueRef a, in FlxValueRef b)
        {
            return a.AsDecimal.CompareTo(b.AsDecimal);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareTo(in FlxValueRef a, in FlxValueRef b)
        {
            if (a.ValueType == b.ValueType)
            {
                return compareFunctions[(int)a.ValueType](a, b);
            }

            return a._type - b._type;
        }
    }
}
