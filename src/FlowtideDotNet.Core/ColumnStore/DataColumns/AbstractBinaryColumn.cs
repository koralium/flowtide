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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    public abstract class AbstractBinaryColumn : IDataColumn
    {
        private static byte[] s_emptyArray = new byte[0];
        protected byte[] _data;
        protected int _length = 0;
        protected List<int> _offsets = new List<int>();

        public AbstractBinaryColumn()
        {
            _data = s_emptyArray;
        }

        private void EnsureCapacity(int length)
        {
            if (_data.Length < length)
            {
                var newData = new byte[length * 2];
                _data.CopyTo(newData, 0);
                _data = newData;
            }
        }

        protected int Add(in Span<byte> value)
        {
            EnsureCapacity(_length + value.Length);
            value.CopyTo(_data.AsSpan(_length));
            var resultOffset = _offsets.Count;
            _offsets.Add(_length);
            _length += value.Length;
            return resultOffset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (int, int) GetStartEndOffset(in int index)
        {
            int startOffset = _offsets[index];
            if (index + 1 < _offsets.Count)
            {
                return (startOffset, _offsets[index + 1]);
            }
            else
            {
                return (startOffset, _length);
            }
        }

        protected void Remove(int index)
        {
            var (startOffset, endOffset) = GetStartEndOffset(in index);
            var length = endOffset - startOffset;
        }

        private void MemCpyWithAdd(Span<int> source, Span<int> destination, int index, int length, int destinationIndex, int addition)
        {
            
            //var additionVector = Vector128.Create(addition); 
            unsafe
            {
                fixed (int* pSrc = source)
                fixed (int* pDst = destination)
                {
                    int i = 0;
                    
                    // Check if we can use AVX512 this will allow moving 16 integers at a time while doing addition
                    if (Avx512F.IsSupported)
                    {
                        int vectorSize = Vector512<int>.Count;
                        var vec512 = Vector512.Create(addition);
                        // Check if we have 64 byte alignment
                        if (((long)pSrc % 64 == 0) && ((long)pDst % 64 == 0))
                        {
                            for (; i <= source.Length - vectorSize; i += vectorSize)
                            {
                                Vector512<int> sourceVector = Avx512F.LoadAlignedVector512(pSrc + i);
                                Vector512<int> resultVector = Avx512F.Add(sourceVector, vec512);
                                Avx512F.Store(pDst + i, resultVector);
                            }
                        }
                        else
                        {
                            // Non aligned
                            for (; i < source.Length - vectorSize; i+= vectorSize)
                            {
                                Vector512<int> sourceVector = Avx512F.LoadVector512(pSrc + i);
                                Vector512<int> resultVector = Avx512F.Add(sourceVector, vec512);
                                Avx512F.Store(pDst + i, resultVector);
                            }
                        }
                    }
                    // Check if the pc has 256 bit support, will move 8 integers at a time.
                    else if (Avx2.IsSupported)
                    {
                        int vectorSize = Vector256<int>.Count;
                        var vec256 = Vector256.Create(addition);

                        // Check that we are aligned
                        if (((long)pSrc % 32 == 0) && ((long)pDst % 32 == 0))
                        {
                            for (; i <= source.Length - vectorSize; i += vectorSize)
                            {
                                Vector256<int> sourceVector = Avx.LoadAlignedVector256(pSrc + i);
                                Vector256<int> resultVector = Avx2.Add(sourceVector, vec256);
                                Avx.StoreAligned(pDst + i, resultVector);
                            }
                        }
                        else
                        {
                            // Non aligned
                            for (; i <= source.Length - vectorSize; i += vectorSize)
                            {
                                Vector256<int> sourceVector = Avx.LoadVector256(pSrc + i);
                                Vector256<int> resultVector = Avx2.Add(sourceVector, vec256);
                                Avx.Store(pDst + i, resultVector);
                            }
                        }
                    }
                    // Check if SSE is enabled on the pc, will move 4 integers at a time.
                    else if (Sse.IsSupported)
                    {
                        int vectorSize = Vector128<int>.Count;
                        var additionVector = Vector128.Create(addition);

                        // Check alignment
                        if (((long)pSrc % 16 == 0) && ((long)pDst % 16 == 0))
                        {
                            for (; i <= source.Length - vectorSize; i += vectorSize)
                            {
                                Vector128<int> sourceVector = Sse2.LoadAlignedVector128(pSrc + i);
                                Vector128<int> resultVector = Sse2.Add(sourceVector, additionVector);
                                Sse2.StoreAligned(pDst + i, resultVector);
                            }
                        }
                        else
                        {
                            for (; i <= source.Length - vectorSize; i += vectorSize)
                            {
                                Vector128<int> sourceVector = Sse2.LoadVector128(pSrc + i);
                                Vector128<int> resultVector = Sse2.Add(sourceVector, additionVector);
                                Sse2.Store(pDst + i, resultVector);
                            }
                        }
                    }

                    // Do remainder, or all rows if no simd operations where possible.
                    for (; i < source.Length; i++)
                    {
                        destination[i] = source[i] + addition;
                    }
                }
            }
        }


        public abstract int Add(in IDataValue value);

        public abstract int CompareToStrict(in int index, in IDataValue value);

        public abstract int CompareToStrict(in IDataColumn otherColumn, in int thisIndex, in int otherIndex);

        public abstract IDataValue GetValueAt(in int index);

        public abstract int Update(in int index, in IDataValue value);

        public int CompareToStrict<T>(in int index, in T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public int Add<T>(in T value) where T : struct, IDataValue
        {
            throw new NotImplementedException();
        }

        public int Update<T>(in int index, in T value) where T : struct, IDataValue
        {
            throw new NotImplementedException();
        }

        public abstract void GetValueAt(in int index, in DataValueContainer dataValueContainer);

        public int BinarySearch(in IDataValue dataValue)
        {
            throw new NotImplementedException();
        }

        public int BinarySearch(in IDataValue dataValue, in int start, in int end)
        {
            throw new NotImplementedException();
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end)
            where T : IDataValue
        {
            throw new NotImplementedException();
        }
    }
}
