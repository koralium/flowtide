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

using Apache.Arrow;
using Apache.Arrow.Types;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{
    /// <summary>
    /// Integer list that stores its values at the smallest bit width that fits them.
    /// Mutable struct, keep it in one field and never copy it.
    /// </summary>
    [NonCopyable]
    internal unsafe struct IntegerList
    {
        // Every NativeList instantiation has the same layout since no field depends on T,
        // so the byte typed storage can be viewed as the current width's list.
        private NativeList<byte> _storage;
        private byte _elementSize;

        public IntegerList(byte elementSize)
        {
            _elementSize = elementSize;
        }

        /// <summary>
        /// Creates a list that wraps already populated memory, taking ownership of it.
        /// </summary>
        public IntegerList(FlowtideMemory memory, int length, byte elementSize)
        {
            _storage = new NativeList<byte>(memory, length);
            _elementSize = elementSize;
        }

        /// <summary>
        /// The element size in bytes, zero when no width has been chosen yet.
        /// </summary>
        public readonly byte ElementSize => _elementSize;

        public readonly int BitWidth => _elementSize == 0 ? 8 : _elementSize * 8;

        public readonly int Count => _storage.Count;

        public readonly long MaxValue => _elementSize switch
        {
            1 => sbyte.MaxValue,
            2 => short.MaxValue,
            4 => int.MaxValue,
            _ => long.MaxValue,
        };

        public readonly long MinValue => _elementSize switch
        {
            1 => sbyte.MinValue,
            2 => short.MinValue,
            4 => int.MinValue,
            _ => long.MinValue,
        };

        public readonly Span<byte> SlicedSpan => new Span<byte>(_storage.GetPointer_Unsafe(), _storage.Count * _elementSize);

        public readonly Memory<byte> SlicedMemory => _storage.Memory.Slice(0, _storage.Count * _elementSize);

        public readonly void* GetPointer_Unsafe()
        {
            return _storage.GetPointer_Unsafe();
        }

        private static ref NativeList<sbyte> AsInt8(ref NativeList<byte> storage) => ref Unsafe.As<NativeList<byte>, NativeList<sbyte>>(ref storage);
        private static ref NativeList<short> AsInt16(ref NativeList<byte> storage) => ref Unsafe.As<NativeList<byte>, NativeList<short>>(ref storage);
        private static ref NativeList<int> AsInt32(ref NativeList<byte> storage) => ref Unsafe.As<NativeList<byte>, NativeList<int>>(ref storage);
        private static ref NativeList<long> AsInt64(ref NativeList<byte> storage) => ref Unsafe.As<NativeList<byte>, NativeList<long>>(ref storage);

        public readonly long Get(int index)
        {
            Debug.Assert(index >= 0 && index < _storage.Count);
            var pointer = _storage.GetPointer_Unsafe();
            return _elementSize switch
            {
                1 => ((sbyte*)pointer)[index],
                2 => ((short*)pointer)[index],
                4 => ((int*)pointer)[index],
                _ => ((long*)pointer)[index],
            };
        }

        public readonly void Update(int index, long value)
        {
            ref var storage = ref Unsafe.AsRef(in _storage);
            switch (_elementSize)
            {
                case 1: AsInt8(ref storage).Update(index, (sbyte)value); break;
                case 2: AsInt16(ref storage).Update(index, (short)value); break;
                case 4: AsInt32(ref storage).Update(index, (int)value); break;
                default: AsInt64(ref storage).Update(index, value); break;
            }
        }

        public int Add(long value, IMemoryAllocator memoryAllocator)
        {
            var index = _storage.Count;
            switch (_elementSize)
            {
                case 1: AsInt8(ref _storage).Add((sbyte)value, memoryAllocator); break;
                case 2: AsInt16(ref _storage).Add((short)value, memoryAllocator); break;
                case 4: AsInt32(ref _storage).Add((int)value, memoryAllocator); break;
                default: AsInt64(ref _storage).Add(value, memoryAllocator); break;
            }
            return index;
        }

        public void InsertAt(int index, long value, IMemoryAllocator memoryAllocator)
        {
            switch (_elementSize)
            {
                case 1: AsInt8(ref _storage).InsertAt(index, (sbyte)value, memoryAllocator); break;
                case 2: AsInt16(ref _storage).InsertAt(index, (short)value, memoryAllocator); break;
                case 4: AsInt32(ref _storage).InsertAt(index, (int)value, memoryAllocator); break;
                default: AsInt64(ref _storage).InsertAt(index, value, memoryAllocator); break;
            }
        }

        public void InsertNullRange(int index, int count, IMemoryAllocator memoryAllocator)
        {
            switch (_elementSize)
            {
                case 1: AsInt8(ref _storage).InsertStaticRange(index, 0, count, memoryAllocator); break;
                case 2: AsInt16(ref _storage).InsertStaticRange(index, 0, count, memoryAllocator); break;
                case 4: AsInt32(ref _storage).InsertStaticRange(index, 0, count, memoryAllocator); break;
                default: AsInt64(ref _storage).InsertStaticRange(index, 0, count, memoryAllocator); break;
            }
        }

        public void RemoveAt(int index, IMemoryAllocator memoryAllocator)
        {
            switch (_elementSize)
            {
                case 1: AsInt8(ref _storage).RemoveAt(index, memoryAllocator); break;
                case 2: AsInt16(ref _storage).RemoveAt(index, memoryAllocator); break;
                case 4: AsInt32(ref _storage).RemoveAt(index, memoryAllocator); break;
                default: AsInt64(ref _storage).RemoveAt(index, memoryAllocator); break;
            }
        }

        public void RemoveRange(int start, int count, IMemoryAllocator memoryAllocator)
        {
            if (count == 0)
            {
                return;
            }
            switch (_elementSize)
            {
                case 1: AsInt8(ref _storage).RemoveRange(start, count, memoryAllocator); break;
                case 2: AsInt16(ref _storage).RemoveRange(start, count, memoryAllocator); break;
                case 4: AsInt32(ref _storage).RemoveRange(start, count, memoryAllocator); break;
                default: AsInt64(ref _storage).RemoveRange(start, count, memoryAllocator); break;
            }
        }

        public void MoveRangeAt(int index, int count, IMemoryAllocator memoryAllocator)
        {
            if (count == 0)
            {
                return;
            }
            switch (_elementSize)
            {
                case 1: AsInt8(ref _storage).MoveAtIndex(index, count, memoryAllocator); break;
                case 2: AsInt16(ref _storage).MoveAtIndex(index, count, memoryAllocator); break;
                case 4: AsInt32(ref _storage).MoveAtIndex(index, count, memoryAllocator); break;
                default: AsInt64(ref _storage).MoveAtIndex(index, count, memoryAllocator); break;
            }
        }

        public void DeleteBatch(ReadOnlySpan<int> targets, IMemoryAllocator memoryAllocator)
        {
            if (targets.Length == 0)
            {
                return;
            }
            switch (_elementSize)
            {
                case 1: AsInt8(ref _storage).DeleteBatch(targets, memoryAllocator); break;
                case 2: AsInt16(ref _storage).DeleteBatch(targets, memoryAllocator); break;
                case 4: AsInt32(ref _storage).DeleteBatch(targets, memoryAllocator); break;
                default: AsInt64(ref _storage).DeleteBatch(targets, memoryAllocator); break;
            }
        }

        public void InsertRangeFrom(int index, in IntegerList other, int start, int count, IMemoryAllocator memoryAllocator)
        {
            if (count == 0)
            {
                return;
            }
            Debug.Assert(_elementSize == other._elementSize);
            ref var otherStorage = ref Unsafe.AsRef(in other._storage);
            switch (_elementSize)
            {
                case 1: AsInt8(ref _storage).InsertRangeFrom(index, in AsInt8(ref otherStorage), start, count, memoryAllocator); break;
                case 2: AsInt16(ref _storage).InsertRangeFrom(index, in AsInt16(ref otherStorage), start, count, memoryAllocator); break;
                case 4: AsInt32(ref _storage).InsertRangeFrom(index, in AsInt32(ref otherStorage), start, count, memoryAllocator); break;
                default: AsInt64(ref _storage).InsertRangeFrom(index, in AsInt64(ref otherStorage), start, count, memoryAllocator); break;
            }
        }

        public void InsertFrom(in IntegerList other, in ReadOnlySpan<int> sortedLookup, in ReadOnlySpan<int> targetPositions, in int lookupNullIndex, IMemoryAllocator memoryAllocator)
        {
            Debug.Assert(_elementSize == other._elementSize);
            ref var otherStorage = ref Unsafe.AsRef(in other._storage);
            switch (_elementSize)
            {
                case 1: AsInt8(ref _storage).InsertFrom(in AsInt8(ref otherStorage), in sortedLookup, in targetPositions, lookupNullIndex, memoryAllocator); break;
                case 2: AsInt16(ref _storage).InsertFrom(in AsInt16(ref otherStorage), in sortedLookup, in targetPositions, lookupNullIndex, memoryAllocator); break;
                case 4: AsInt32(ref _storage).InsertFrom(in AsInt32(ref otherStorage), in sortedLookup, in targetPositions, lookupNullIndex, memoryAllocator); break;
                default: AsInt64(ref _storage).InsertFrom(in AsInt64(ref otherStorage), in sortedLookup, in targetPositions, lookupNullIndex, memoryAllocator); break;
            }
        }

        public void EnsureCapacity(int count, IMemoryAllocator memoryAllocator)
        {
            switch (_elementSize)
            {
                case 1: AsInt8(ref _storage).EnsureCapacity(count, memoryAllocator); break;
                case 2: AsInt16(ref _storage).EnsureCapacity(count, memoryAllocator); break;
                case 4: AsInt32(ref _storage).EnsureCapacity(count, memoryAllocator); break;
                default: AsInt64(ref _storage).EnsureCapacity(count, memoryAllocator); break;
            }
        }

        public void Clear()
        {
            _storage.Clear();
        }

        /// <summary>
        /// Changes the element size, converting the stored values to the wider width.
        /// </summary>
        public void Widen(byte newElementSize, IMemoryAllocator memoryAllocator)
        {
            if (newElementSize == _elementSize)
            {
                return;
            }
            if (_elementSize == 0 || _storage.Count == 0)
            {
                _storage.Dispose(memoryAllocator);
                _elementSize = newElementSize;
                return;
            }
            switch ((_elementSize, newElementSize))
            {
                case (1, 2): WidenCore<sbyte, short>(newElementSize, memoryAllocator); break;
                case (1, 4): WidenCore<sbyte, int>(newElementSize, memoryAllocator); break;
                case (1, 8): WidenCore<sbyte, long>(newElementSize, memoryAllocator); break;
                case (2, 4): WidenCore<short, int>(newElementSize, memoryAllocator); break;
                case (2, 8): WidenCore<short, long>(newElementSize, memoryAllocator); break;
                case (4, 8): WidenCore<int, long>(newElementSize, memoryAllocator); break;
                default: throw new InvalidOperationException($"Cannot change element size from {_elementSize} to {newElementSize}");
            }
        }

        private void WidenCore<TFrom, TTo>(byte newElementSize, IMemoryAllocator memoryAllocator)
            where TFrom : unmanaged, INumber<TFrom>
            where TTo : unmanaged, INumber<TTo>
        {
            var count = _storage.Count;
            var newList = new NativeList<TTo>();
            newList.EnsureCapacity(count, memoryAllocator);
            var source = (TFrom*)_storage.GetPointer_Unsafe();
            var destination = newList.GetPointer_Unsafe();
            for (int i = 0; i < count; i++)
            {
                destination[i] = TTo.CreateTruncating(source[i]);
            }
            newList.SetLength(count);
            _storage.Dispose(memoryAllocator);
#pragma warning disable RS0042 // The local moves into this instance and is not used again.
            _storage = Unsafe.As<NativeList<TTo>, NativeList<byte>>(ref newList);
#pragma warning restore RS0042
            _elementSize = newElementSize;
        }

        public readonly IntegerList Copy(IMemoryAllocator memoryAllocator)
        {
            var memory = memoryAllocator.AllocateMemory(_storage.Count * _elementSize);
            SlicedSpan.CopyTo(memory.Span);
            return new IntegerList(memory, _storage.Count, _elementSize);
        }

        public void Dispose(IMemoryAllocator memoryAllocator)
        {
            _storage.Dispose(memoryAllocator);
            _elementSize = 0;
        }

        public readonly (int, int) SearchBoundries(in long dataValue, in int start, in int end, bool desc)
        {
            ref var storage = ref Unsafe.AsRef(in _storage);
            switch (_elementSize)
            {
                case 1:
                    if (desc)
                    {
                        if (dataValue < sbyte.MinValue)
                        {
                            var index = ~(end + 1);
                            return (index, index);
                        }
                        if (dataValue > sbyte.MaxValue)
                        {
                            return (~start, ~start);
                        }
                        return BoundarySearch.SearchBoundries(in AsInt8(ref storage), (sbyte)dataValue, start, end, Int8ComparerDesc.Instance);
                    }
                    else
                    {
                        if (dataValue < sbyte.MinValue)
                        {
                            return (~start, ~start);
                        }
                        if (dataValue > sbyte.MaxValue)
                        {
                            var index = ~(end + 1);
                            return (index, index);
                        }
                        return BoundarySearch.SearchBoundries(in AsInt8(ref storage), (sbyte)dataValue, start, end, Int8Comparer.Instance);
                    }
                case 2:
                    if (desc)
                    {
                        if (dataValue < short.MinValue)
                        {
                            var index = ~(end + 1);
                            return (index, index);
                        }
                        if (dataValue > short.MaxValue)
                        {
                            return (~start, ~start);
                        }
                        return BoundarySearch.SearchBoundries(in AsInt16(ref storage), (short)dataValue, start, end, Int16ComparerDesc.Instance);
                    }
                    else
                    {
                        if (dataValue < short.MinValue)
                        {
                            return (~start, ~start);
                        }
                        if (dataValue > short.MaxValue)
                        {
                            var index = ~(end + 1);
                            return (index, index);
                        }
                        return BoundarySearch.SearchBoundries(in AsInt16(ref storage), (short)dataValue, start, end, Int16Comparer.Instance);
                    }
                case 4:
                    if (desc)
                    {
                        if (dataValue < int.MinValue)
                        {
                            var index = ~(end + 1);
                            return (index, index);
                        }
                        if (dataValue > int.MaxValue)
                        {
                            return (~start, ~start);
                        }
                        return BoundarySearch.SearchBoundries(in AsInt32(ref storage), (int)dataValue, start, end, Int32ComparerDesc.Instance);
                    }
                    else
                    {
                        if (dataValue < int.MinValue)
                        {
                            return (~start, ~start);
                        }
                        if (dataValue > int.MaxValue)
                        {
                            var index = ~(end + 1);
                            return (index, index);
                        }
                        return BoundarySearch.SearchBoundriesAsc(AsInt32(ref storage).GetPointer_Unsafe(), (int)dataValue, start, in end);
                    }
                default:
                    if (desc)
                    {
                        return BoundarySearch.SearchBoundries(in AsInt64(ref storage), dataValue, start, end, Int64ComparerDesc.Instance);
                    }
                    else
                    {
                        return BoundarySearch.SearchBoundries(in AsInt64(ref storage), dataValue, start, end, Int64Comparer.Instance);
                    }
            }
        }

        public readonly (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            var valueBuffer = new ArrowBuffer(SlicedMemory);
            return _elementSize switch
            {
                1 => ((IArrowArray, IArrowType))(new Int8Array(valueBuffer, nullBuffer, _storage.Count, nullCount, 0), Int8Type.Default),
                2 => (new Int16Array(valueBuffer, nullBuffer, _storage.Count, nullCount, 0), Int16Type.Default),
                4 => (new Int32Array(valueBuffer, nullBuffer, _storage.Count, nullCount, 0), Int32Type.Default),
                _ => (new Int64Array(valueBuffer, nullBuffer, _storage.Count, nullCount, 0), Int64Type.Default),
            };
        }

        public readonly void SetSelfComparePointers(ref SelfComparePointers selfComparePointers)
        {
            selfComparePointers.dataPointer = _storage.GetPointer_Unsafe();
        }

        public readonly System.Linq.Expressions.Expression CreateSelfCompareExpression(
            System.Linq.Expressions.Expression selfComparePointerExpression,
            System.Linq.Expressions.Expression xExpression,
            System.Linq.Expressions.Expression yExpression)
        {
            return _elementSize switch
            {
                1 => NativeSortHelpers.CallCompareInt8(selfComparePointerExpression, xExpression, yExpression),
                2 => NativeSortHelpers.CallCompareInt16(selfComparePointerExpression, xExpression, yExpression),
                4 => NativeSortHelpers.CallCompareInt32(selfComparePointerExpression, xExpression, yExpression),
                _ => NativeSortHelpers.CallCompareInt64(selfComparePointerExpression, xExpression, yExpression),
            };
        }

        public readonly CompareColumnState GetColumnState()
        {
            return _elementSize switch
            {
                0 => CompareColumnStateBuilder.Create(ArrowTypeId.Int8),
                1 => CompareColumnStateBuilder.Create(ArrowTypeId.Int8),
                2 => CompareColumnStateBuilder.Create(ArrowTypeId.Int16),
                4 => CompareColumnStateBuilder.Create(ArrowTypeId.Int32),
                _ => CompareColumnStateBuilder.Create(ArrowTypeId.Int64),
            };
        }

        public readonly int SetRadixPrefix(Span<RadixItem> items, int insertBytePosition, ReadOnlySpan<int> selectionVector)
        {
            ref RadixItem itemsRef = ref MemoryMarshal.GetReference(items);
            var pointer = _storage.GetPointer_Unsafe();
            switch (_elementSize)
            {
                case 1:
                {
                    ReadOnlySpan<sbyte> span = new ReadOnlySpan<sbyte>(pointer, _storage.Count);
                    if (selectionVector.IsEmpty)
                    {
                        for (int i = 0; i < items.Length; i++)
                        {
                            ref RadixItem item = ref Unsafe.Add(ref itemsRef, i);
                            sbyte rawValue = span[item.Index];
                            byte alignedValue = (byte)(rawValue ^ 0x80);
                            ref byte byteOffset = ref Unsafe.Add(ref Unsafe.As<RadixItem, byte>(ref item), insertBytePosition);
                            byteOffset = alignedValue;
                        }
                    }
                    else
                    {
                        for (int i = 0; i < items.Length; i++)
                        {
                            ref RadixItem item = ref Unsafe.Add(ref itemsRef, i);
                            int selectedIndex = selectionVector[item.Index];

                            ref byte byteOffset = ref Unsafe.Add(ref Unsafe.As<RadixItem, byte>(ref item), insertBytePosition);

                            if (selectedIndex >= 0)
                            {
                                sbyte rawValue = span[selectedIndex];
                                byteOffset = (byte)(rawValue ^ 0x80);
                            }
                            else
                            {
                                byteOffset = 0;
                            }
                        }
                    }
                    return 1;
                }
                case 2:
                {
                    ReadOnlySpan<short> span = new ReadOnlySpan<short>(pointer, _storage.Count);
                    if (selectionVector.IsEmpty)
                    {
                        for (int i = 0; i < items.Length; i++)
                        {
                            ref RadixItem item = ref Unsafe.Add(ref itemsRef, i);
                            short rawValue = span[item.Index];
                            ushort alignedValue = (ushort)(rawValue ^ 0x8000);

                            ref byte byteOffset = ref Unsafe.Add(ref Unsafe.As<RadixItem, byte>(ref item), insertBytePosition);
                            Unsafe.As<byte, ushort>(ref byteOffset) = alignedValue;
                        }
                    }
                    else
                    {
                        for (int i = 0; i < items.Length; i++)
                        {
                            ref RadixItem item = ref Unsafe.Add(ref itemsRef, i);
                            int selectedIndex = selectionVector[item.Index];

                            ref byte byteOffset = ref Unsafe.Add(ref Unsafe.As<RadixItem, byte>(ref item), insertBytePosition);

                            if (selectedIndex >= 0)
                            {
                                short rawValue = span[selectedIndex];
                                ushort alignedValue = (ushort)(rawValue ^ 0x8000);
                                Unsafe.As<byte, ushort>(ref byteOffset) = alignedValue;
                            }
                            else
                            {
                                Unsafe.As<byte, ushort>(ref byteOffset) = 0;
                            }
                        }
                    }
                    return 2;
                }
                case 4:
                {
                    ReadOnlySpan<int> span = new ReadOnlySpan<int>(pointer, _storage.Count);
                    if (selectionVector.IsEmpty)
                    {
                        for (int i = 0; i < items.Length; i++)
                        {
                            ref RadixItem item = ref Unsafe.Add(ref itemsRef, i);
                            int rawValue = span[item.Index];

                            uint alignedValue = (uint)rawValue ^ 0x80000000u;

                            ref byte byteOffset = ref Unsafe.Add(ref Unsafe.As<RadixItem, byte>(ref item), insertBytePosition);
                            Unsafe.As<byte, uint>(ref byteOffset) = alignedValue;
                        }
                    }
                    else
                    {
                        for (int i = 0; i < items.Length; i++)
                        {
                            ref RadixItem item = ref Unsafe.Add(ref itemsRef, i);
                            int selectedIndex = selectionVector[item.Index];

                            ref byte byteOffset = ref Unsafe.Add(ref Unsafe.As<RadixItem, byte>(ref item), insertBytePosition);

                            if (selectedIndex >= 0)
                            {
                                int rawValue = span[selectedIndex];
                                uint alignedValue = (uint)rawValue ^ 0x80000000u;
                                Unsafe.As<byte, uint>(ref byteOffset) = alignedValue;
                            }
                            else
                            {
                                Unsafe.As<byte, uint>(ref byteOffset) = 0u;
                            }
                        }
                    }
                    return 4;
                }
                default:
                {
                    ReadOnlySpan<long> span = new ReadOnlySpan<long>(pointer, _storage.Count);
                    if (selectionVector.IsEmpty)
                    {
                        for (int i = 0; i < items.Length; i++)
                        {
                            ref RadixItem item = ref Unsafe.Add(ref itemsRef, i);
                            long rawValue = span[item.Index];

                            ulong alignedValue = (ulong)rawValue ^ 0x8000000000000000ul;

                            ref byte byteOffset = ref Unsafe.Add(ref Unsafe.As<RadixItem, byte>(ref item), insertBytePosition);
                            Unsafe.As<byte, ulong>(ref byteOffset) = alignedValue;
                        }
                    }
                    else
                    {
                        for (int i = 0; i < items.Length; i++)
                        {
                            ref RadixItem item = ref Unsafe.Add(ref itemsRef, i);
                            int selectedIndex = selectionVector[item.Index];

                            ref byte byteOffset = ref Unsafe.Add(ref Unsafe.As<RadixItem, byte>(ref item), insertBytePosition);

                            if (selectedIndex >= 0)
                            {
                                long rawValue = span[selectedIndex];
                                ulong alignedValue = (ulong)rawValue ^ 0x8000000000000000ul;
                                Unsafe.As<byte, ulong>(ref byteOffset) = alignedValue;
                            }
                            else
                            {
                                Unsafe.As<byte, ulong>(ref byteOffset) = 0ul;
                            }
                        }
                    }
                    return 8;
                }
            }
        }
    }
}
