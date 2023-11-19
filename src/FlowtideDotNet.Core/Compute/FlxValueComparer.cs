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

        public static int CompareTo(FlxValue a, FlxValue b)
        {
            var tComp = a.ValueType.CompareTo(b.ValueType);
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
                    return string.Compare(a.AsString, b.AsString);
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
                throw new NotImplementedException();
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
                throw new NotImplementedException();
            }
            if (a._type < b._type)
            {
                return -1;
            }
            return 1;
        }
    }
}
