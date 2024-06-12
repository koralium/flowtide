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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal class BitmapArrayOld
    {
        static readonly byte[] _emptyArray = new byte[0];
        private byte[] _data;

        public BitmapArrayOld()
        {
            
            _data = _emptyArray;
        }

        public void RemoveAt(in int index)
        {
            var byteIndex = index / 8;
            var bitIndex = index % 8;

            int start = byteIndex + 1;
            var span = _data.AsSpan<byte>();

            var existing = span[index];
            


            //var ulongspan = MemoryMarshal.Cast<byte, ulong>(span);
            //ulong endBitMask = ((ulong)1) << 63;
            //ulong firstBitMask = ((ulong)1);
            //for (int i = start; i < ulongspan.Length; i++)
            //{
            //    if ((ulongspan[i] & endBitMask) > 0)
            //    {
            //        // Add the bit to the previous byte at the end
            //        ulongspan[i - 1] = ulongspan[i - 1] | firstBitMask;
            //    }
            //    ulongspan[i] = ulongspan[i] << 1;
            //}
        }

        public void Set(in int index)
        {
            if (index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var byteIndex = index / 8;
            var bitIndex = index % 8;

            if (byteIndex >= _data.Length)
            {
                var newData = new byte[byteIndex + 1];
                Array.Copy(_data, newData, _data.Length);
                _data = newData;
            }

            _data[byteIndex] |= (byte)(1 << bitIndex);
        }

        public void Clear(in int index)
        {
            if (index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var byteIndex = index / 8;
            var bitIndex = index % 8;

            if (byteIndex >= _data.Length)
            {
                return;
            }

            _data[byteIndex] &= (byte)~(1 << bitIndex);
        }

        public bool IsSet(in int index)
        {
            Debug.Assert(index >= 0);

            var byteIndex = index / 8;
            var bitIndex = index % 8;

            if (byteIndex >= _data.Length)
            {
                return false;
            }

            return (_data[byteIndex] & (byte)(1 << bitIndex)) != 0;
        }
    }
}
