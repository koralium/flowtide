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
using FlexBuffers;
using SqlParser.Ast;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Core.Compute
{
    internal static class FlxValueComparer
    {
        public static bool Equal(FlxValue a, FlxValue b)
        {
            if (a.ValueType == FlexBuffers.Type.String && b.ValueType == FlexBuffers.Type.String)
            {
                return a.AsString.Equals(b.AsString, StringComparison.OrdinalIgnoreCase);
            }
            return CompareTo(a, b) == 0;
        }

        internal static int CompareDecimal(FlxValue dec, FlxValue b)
        {
            if (b.ValueType == FlexBuffers.Type.Decimal)
            {
                return dec.AsDecimal.CompareTo(b.AsDecimal);
            }
            if (b.ValueType == FlexBuffers.Type.Float)
            {
                return dec.AsDecimal.CompareTo((decimal)b.AsDouble);
            }
            if (b.ValueType == FlexBuffers.Type.Int) 
            {
                return dec.AsDecimal.CompareTo((decimal)b.AsDouble);
            }
            return dec.ValueType - b.ValueType;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CompareLongSameWidth(in FlxValue a, in FlxValue b, in byte width) 
        {
            if (width == 1)
            {
                return a.Buffer[a._offset] - b.Buffer[b._offset];
            }

            if (width == 2)
            {
                return BitConverter.ToInt16(a.Buffer.Slice(a._offset, 2)) - BitConverter.ToInt16(b.Buffer.Slice(b._offset, 2));
            }

            if (width == 4)
            {
                return BitConverter.ToInt32(a.Buffer.Slice(a._offset, 4)) - BitConverter.ToInt32(b.Buffer.Slice(b._offset, 4));
            }

            return BitConverter.ToInt64(a.Buffer.Slice(a._offset, 8)).CompareTo(BitConverter.ToInt64(b.Buffer.Slice(b._offset, 8)));
        }

        public static int CompareTo(in FlxValue a, in FlxValue b)
        {
            var tComp = a.ValueType - b.ValueType;
            if (tComp == 0)
            {
                // Same tpe
                if (a.ValueType == FlexBuffers.Type.Null)
                {
                    return 0;
                }
                if (a.ValueType == FlexBuffers.Type.Bool)
                {
                    return a.AsBool.CompareTo(b.AsBool);
                }
                // Check for string comparison
                if (a.ValueType == FlexBuffers.Type.String)
                {
                    return FlxString.Compare(a.AsFlxString, b.AsFlxString);
                }
                if (a.ValueType == FlexBuffers.Type.Int)
                {
                    if (a._parentWidth == b._parentWidth)
                    {
                        return CompareLongSameWidth(in a, in b, in a._parentWidth);
                    }
                    return a.AsLong.CompareTo(b.AsLong);
                }
                if (a.ValueType == FlexBuffers.Type.Key)
                {
                    return string.Compare(a.AsString, b.AsString);
                }
                if (a.ValueType == FlexBuffers.Type.Vector)
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
                if (a.ValueType == FlexBuffers.Type.Blob)
                {
                    var ablob = a.AsBlob;
                    var bblob = b.AsBlob;
                    return ablob.SequenceCompareTo(bblob);
                }
                if (a.ValueType == FlexBuffers.Type.Map)
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
                throw new NotImplementedException();
            }
            if (a.ValueType == FlexBuffers.Type.Decimal)
            {
                return CompareDecimal(a, b);
            }
            if (b.ValueType == FlexBuffers.Type.Decimal)
            {
                return CompareDecimal(b, a) * -1;
            }
            return tComp;
        }
    }

    internal static class FlxValueRefComparer
    {
        public static bool Equal(in FlxValueRef a, in FlxValueRef b)
        {
            if (a.ValueType == FlexBuffers.Type.String && b.ValueType == FlexBuffers.Type.String)
            {
                return a.AsString.Equals(b.AsString, StringComparison.OrdinalIgnoreCase);
            }
            return CompareTo(a, b) == 0;
        }

        internal static int CompareDecimal(scoped in FlxValueRef dec, scoped in FlxValueRef b)
        {
            if (b.ValueType == FlexBuffers.Type.Decimal)
            {
                return dec.AsDecimal.CompareTo(b.AsDecimal);
            }
            if (b.ValueType == FlexBuffers.Type.Float)
            {
                return dec.AsDecimal.CompareTo((decimal)b.AsDouble);
            }
            if (b.ValueType == FlexBuffers.Type.Int)
            {
                return dec.AsDecimal.CompareTo((decimal)b.AsDouble);
            }
            return dec.ValueType - b.ValueType;
        }

        public static int CompareTo(in FlxValueRef a, in FlxValueRef b)
        {
            if (a._type == b._type)
            {
                // Same tpe
                if (a.ValueType == FlexBuffers.Type.Null)
                {
                    return 0;
                }
                if (a.ValueType == FlexBuffers.Type.Bool)
                {
                    return a.AsBool.CompareTo(b.AsBool);
                }
                // Check for string comparison
                if (a.ValueType == FlexBuffers.Type.String)
                {
                    return FlxString.Compare(a.AsFlxString, b.AsFlxString);
                    //return string.Compare(a.AsString, b.AsString, StringComparison.OrdinalIgnoreCase);
                }
                if (a.ValueType == FlexBuffers.Type.Int)
                {
                    return a.AsLong.CompareTo(b.AsLong);
                }
                if (a.ValueType == FlexBuffers.Type.Vector)
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
                if (a.ValueType == FlexBuffers.Type.Map)
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
                if (a.ValueType == FlexBuffers.Type.Key)
                {
                    return string.Compare(a.AsString, b.AsString);
                }
                if (a.ValueType == FlexBuffers.Type.Blob)
                {
                    var ablob = a.AsBlob;
                    var bblob = b.AsBlob;
                    return ablob.SequenceCompareTo(bblob);
                }
                if (a.ValueType == FlexBuffers.Type.Decimal)
                {
                    return decimal.Compare(a.AsDecimal, b.AsDecimal);
                }
                if (a.ValueType == FlexBuffers.Type.Float)
                {
                    return a.AsDouble.CompareTo(b.AsDouble);
                }
                throw new NotImplementedException();
            }
            if (a.ValueType == FlexBuffers.Type.Decimal)
            {
                return CompareDecimal(a, b);
            }
            if (b.ValueType == FlexBuffers.Type.Decimal)
            {
                return CompareDecimal(b, a) * -1;
            }
            if (a._type < b._type)
            {
                return -1;
            }
            return 1;
        }
    }
}
