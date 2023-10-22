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

using System.Runtime.InteropServices;

namespace FlexBuffers
{
    [StructLayout(LayoutKind.Explicit, Size=10)]
    public struct StackValue
    {
        [FieldOffset(0)] private ulong UValue;
        
        [FieldOffset(0)] private long LValue;
        
        [FieldOffset(0)] private double DValue;

        [FieldOffset(8)] private BitWidth Width;

        [FieldOffset(9)] private Type ValueType;

        public static StackValue Null()
        {
            return new StackValue
            {
                Width = BitWidth.Width8,
                LValue = 0,
                ValueType = Type.Null
            };
        }
        
        public static StackValue Value(float value)
        {
            return new StackValue
            {
                Width = BitWidth.Width32,
                DValue = value,
                ValueType = Type.Float
            };
        }
        
        public static StackValue Value(double value)
        {
            return new StackValue
            {
                Width = BitWidthUtil.Width(value),
                DValue = value,
                ValueType = Type.Float
            };
        }
        
        public static StackValue Value(bool value)
        {
            return new StackValue
            {
                Width = BitWidth.Width8,
                LValue = value ? 1 : 0,
                ValueType = Type.Bool
            };
        }
        
        public static StackValue Value(long value)
        {
            return new StackValue
            {
                Width = BitWidthUtil.Width(value),
                LValue = value,
                ValueType = Type.Int
            };
        }
        
        public static StackValue Value(ulong value)
        {
            return new StackValue
            {
                Width = BitWidthUtil.Width(value),
                UValue = value,
                ValueType = Type.Uint
            };
        }
        
        public static StackValue Value(ulong value, BitWidth width, Type type)
        {
            return new StackValue
            {
                Width = width,
                UValue = value,
                ValueType = type
            };
        }
        
        public static StackValue Value(long value, BitWidth width, Type type)
        {
            return new StackValue
            {
                Width = width,
                LValue = value,
                ValueType = type
            };
        }

        public BitWidth StoredWidth(BitWidth bitWidth = BitWidth.Width8)
        {
            if (TypesUtil.IsInline(ValueType))
            {
                return (BitWidth) Math.Max((int) bitWidth, (int) Width);
            }

            return Width;
        }

        public byte StoredPackedType(BitWidth bitWidth = BitWidth.Width8)
        {
            return TypesUtil.PackedType(ValueType, StoredWidth(bitWidth));
        }

        public BitWidth ElementWidth(ulong size, int index)
        {
            if (TypesUtil.IsInline(ValueType))
            {
                return Width;
            }

            for (var i = 0; i < 4; i++)
            {
                var width = (ulong)1 << i;
                var offsetLoc = size + BitWidthUtil.PaddingSize(size, width) + (ulong)index * width;
                var offset = offsetLoc - UValue;
                var bitWidth = BitWidthUtil.Width(offset);
                if ((1UL << (byte) bitWidth) == width)
                {
                    return bitWidth;
                }
            }
            throw new Exception($"Element with size: {size} and index: {index} is of unknown width");
        }

        public long AsLong => LValue;
        public ulong AsULong => UValue;
        public double AsDouble => DValue;

        public bool IsFloat32 => ValueType == Type.Float && Width == BitWidth.Width32;
        public bool IsOffset => TypesUtil.IsInline(ValueType) == false;

        public Type TypeOfValue => ValueType;
        public BitWidth InternalWidth => Width;
    }
}