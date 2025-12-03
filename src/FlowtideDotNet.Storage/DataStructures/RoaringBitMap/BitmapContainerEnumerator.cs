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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.DataStructures.RoaringBitMap
{
    internal class BitmapContainerEnumerator : IEnumerator<ushort>, IContainerEnumerator
    {
        long w;
        int x;

        long[] bitmap;

        public BitmapContainerEnumerator(long[] bitmap)
        {
            Wrap(bitmap);
        }

        [MemberNotNull(nameof(bitmap))]
        public void Wrap(long[] b)
        {
            bitmap = b;
            for (x = 0; x < bitmap.Length; ++x)
            {
                if ((w = bitmap[x]) != 0)
                {
                    break;
                }
            }
        }

        public ushort Current { get; set; }

        public bool HasNext()
        {
            return x < bitmap.Length;
        }

        public ushort Next()
        {
            char answer = (char)(x * 64 + BitOperations.TrailingZeroCount(w));
            w &= (w - 1);
            while (w == 0)
            {
                ++x;
                if (x == bitmap.Length)
                {
                    break;
                }
                w = bitmap[x];
            }
            return answer;
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }

        public bool MoveNext()
        {
            if (HasNext())
            {
                Current = Next();
                return true;
            }
            return false;
        }

        public void Reset()
        {
            x = 0;
            w = 0;
            Wrap(bitmap);
        }
    }
}
