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
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Set.Structs
{
    internal interface IInputWeight
    {
        void SetValue(int index, int value);

        int GetValue(int index);

        bool IsAllZero();

        int Count { get; }
    }

    public static class InputWeight
    {
        public static Type GetType(int count)
        {
            return count switch
            {
                1 => typeof(InputWeights1),
                2 => typeof(InputWeights2),
                3 => typeof(InputWeights3),
                4 => typeof(InputWeights4),
                5 => typeof(InputWeights5),
                6 => typeof(InputWeights6),
                7 => typeof(InputWeights7),
                8 => typeof(InputWeights8),
                _ => throw new NotSupportedException($"Input weight count {count} is not supported")
            };
        }
    }

    [StructLayout(LayoutKind.Explicit)]
    internal unsafe struct InputWeights1 : IInputWeight
    {
        [FieldOffset(0)]
        public int weight1;

        public int Count => 1;

        public void Add<TStruct>(TStruct other)
        {
        }

        public int GetValue(int index)
        {
            fixed (int* p = &weight1)
            {
                return p[index];
            }
        }

        public bool IsAllZero()
        {
            return weight1 == 0;
        }

        public void SetValue(int index, int value)
        {
            fixed (int* p = &weight1)
            {
                p[index] = value;
            }
        }
    }

    [StructLayout(LayoutKind.Explicit)]
    internal unsafe struct InputWeights2 : IInputWeight
    {
        [FieldOffset(0)]
        public int weight1;

        [FieldOffset(4)]
        public int weight2;

        public int Count => 2;

        public int GetValue(int index)
        {
            fixed (int* p = &weight1)
            {
                return p[index];
            }
        }

        public bool IsAllZero()
        {
            return weight1 == 0 && weight2 == 0;
        }

        public void SetValue(int index, int value)
        {
            fixed (int* p = &weight1)
            {
                p[index] = value;
            }
        }
    }

    [StructLayout(LayoutKind.Explicit)]
    internal unsafe struct InputWeights3 : IInputWeight
    {
        [FieldOffset(0)]
        public int weight1;

        [FieldOffset(4)]
        public int weight2;

        [FieldOffset(8)]
        public int weight3;

        public int Count => 3;

        public int GetValue(int index)
        {
            fixed (int* p = &weight1)
            {
                return p[index];
            }
        }

        public bool IsAllZero()
        {
            return weight1 == 0 && weight2 == 0 && weight3 == 0;
        }

        public void SetValue(int index, int value)
        {
            fixed (int* p = &weight1)
            {
                p[index] = value;
            }
        }
    }


    [StructLayout(LayoutKind.Explicit)]
    internal unsafe struct InputWeights4 : IInputWeight
    {
        [FieldOffset(0)]
        public int weight1;

        [FieldOffset(4)]
        public int weight2;

        [FieldOffset(8)]
        public int weight3;

        [FieldOffset(12)]
        public int weight4;

        public int Count => 4;

        public int GetValue(int index)
        {
            fixed (int* p = &weight1)
            {
                return p[index];
            }
        }

        public bool IsAllZero()
        {
            return weight1 == 0 && weight2 == 0 && weight3 == 0 && weight4 == 0;
        }

        public void SetValue(int index, int value)
        {
            fixed (int* p = &weight1)
            {
                p[index] = value;
            }
        }
    }

    [StructLayout(LayoutKind.Explicit)]
    internal unsafe struct InputWeights5 : IInputWeight
    {
        [FieldOffset(0)]
        public int weight1;

        [FieldOffset(4)]
        public int weight2;

        [FieldOffset(8)]
        public int weight3;

        [FieldOffset(12)]
        public int weight4;

        [FieldOffset(16)]
        public int weight5;

        public int Count => 5;

        public int GetValue(int index)
        {
            fixed (int* p = &weight1)
            {
                return p[index];
            }
        }

        public bool IsAllZero()
        {
            return weight1 == 0 && weight2 == 0 && weight3 == 0 && weight4 == 0 && weight5 == 0;
        }

        public void SetValue(int index, int value)
        {
            fixed (int* p = &weight1)
            {
                p[index] = value;
            }
        }
    }

    [StructLayout(LayoutKind.Explicit)]
    internal unsafe struct InputWeights6 : IInputWeight
    {
        [FieldOffset(0)]
        public int weight1;

        [FieldOffset(4)]
        public int weight2;

        [FieldOffset(8)]
        public int weight3;

        [FieldOffset(12)]
        public int weight4;

        [FieldOffset(16)]
        public int weight5;

        [FieldOffset(20)]
        public int weight6;

        public int Count => 6;

        public int GetValue(int index)
        {
            fixed (int* p = &weight1)
            {
                return p[index];
            }
        }

        public bool IsAllZero()
        {
            return weight1 == 0 && weight2 == 0 && weight3 == 0 && weight4 == 0 && weight5 == 0 && weight6 == 0;
        }

        public void SetValue(int index, int value)
        {
            fixed (int* p = &weight1)
            {
                p[index] = value;
            }
        }
    }

    [StructLayout(LayoutKind.Explicit)]
    internal unsafe struct InputWeights7 : IInputWeight
    {
        [FieldOffset(0)]
        public int weight1;

        [FieldOffset(4)]
        public int weight2;

        [FieldOffset(8)]
        public int weight3;

        [FieldOffset(12)]
        public int weight4;

        [FieldOffset(16)]
        public int weight5;

        [FieldOffset(20)]
        public int weight6;

        [FieldOffset(24)]
        public int weight7;

        public int Count => 7;

        public int GetValue(int index)
        {
            fixed (int* p = &weight1)
            {
                return p[index];
            }
        }

        public bool IsAllZero()
        {
            return weight1 == 0 && weight2 == 0 && weight3 == 0 && weight4 == 0 && weight5 == 0 && weight6 == 0 && weight7 == 0;
        }

        public void SetValue(int index, int value)
        {
            fixed (int* p = &weight1)
            {
                p[index] = value;
            }
        }
    }

    [StructLayout(LayoutKind.Explicit)]
    internal unsafe struct InputWeights8 : IInputWeight
    {
        [FieldOffset(0)]
        public int weight1;

        [FieldOffset(4)]
        public int weight2;

        [FieldOffset(8)]
        public int weight3;

        [FieldOffset(12)]
        public int weight4;

        [FieldOffset(16)]
        public int weight5;

        [FieldOffset(20)]
        public int weight6;

        [FieldOffset(24)]
        public int weight7;

        [FieldOffset(28)]
        public int weight8;

        public int Count => 8;

        public int GetValue(int index)
        {
            fixed (int* p = &weight1)
            {
                return p[index];
            }
        }

        public bool IsAllZero()
        {
            return weight1 == 0 && weight2 == 0 && weight3 == 0 && weight4 == 0 && weight5 == 0 && weight6 == 0 && weight7 == 0 && weight8 == 0;
        }

        public void SetValue(int index, int value)
        {
            fixed (int* p = &weight1)
            {
                p[index] = value;
            }
        }
    }
}
