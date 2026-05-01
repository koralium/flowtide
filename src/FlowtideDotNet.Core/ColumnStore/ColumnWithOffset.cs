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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.IO.Hashing;
using System.Text.Json;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal sealed class ColumnWithOffset : IColumn
    {
        private readonly IColumn innerColumn;
        private readonly PrimitiveList<int> offsets;

        public const int NullValueIndex = -1;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="innerColumn"></param>
        /// <param name="offsets"></param>
        /// <param name="includeNullValueAtEnd">Adds an extra index at the end which always gives out null, useful
        /// when doing left joins or similar to easily add null without needing to modify the inner data columns.
        /// Since these can be used by different operators, all data would need to be copied.</param>
        public ColumnWithOffset(IColumn innerColumn, PrimitiveList<int> offsets)
        {
            this.innerColumn = innerColumn;
            this.offsets = offsets;
        }

        public int Count => offsets.Count;

        public ArrowTypeId Type => innerColumn.Type;

        IDataColumn IColumn.DataColumn => innerColumn.DataColumn;

        StructHeader? IColumn.StructHeader => innerColumn.StructHeader;

        public PrimitiveList<int> Offsets => offsets;

        public IColumn InnerColumn => innerColumn;

        public void Add<T>(in T value) where T : IDataValue
        {
            throw new NotSupportedException("Column with offset does not support add.");
        }

        public int CompareTo<T>(in int index, in T dataValue, in ReferenceSegment? child) where T : IDataValue
        {
            var offset = offsets[index];
            return innerColumn.CompareTo(offset, dataValue, child);
        }

        public int CompareTo(in IColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            var offset = offsets[thisIndex];
            return innerColumn.CompareTo(otherColumn, offset, otherIndex);
        }

        public void Dispose()
        {
            innerColumn.Dispose();
        }

        public int GetByteSize()
        {
            return innerColumn.GetByteSize();
        }

        public int GetByteSize(int start, int end)
        {
            int size = 0;
            for (int i = start; i <= end; i++)
            {
                var offset = offsets[i];
                if (offset == NullValueIndex)
                {
                    size += 0;
                }
                else
                {
                    size += innerColumn.GetByteSize(offset, offset);
                }
            }
            return size;
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            var offset = offsets[index];
            if (offset == NullValueIndex)
            {
                return ArrowTypeId.Null;
            }
            return innerColumn.GetTypeAt(offset, child);
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            var offset = offsets.Get(index);
            if (offset == NullValueIndex)
            {
                return NullValue.Instance;
            }
            return innerColumn.GetValueAt(offset, child); 
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            var offset = offsets.Get(index);
            if (offset == NullValueIndex)
            {
                dataValueContainer._type = ArrowTypeId.Null;
                return;
            }
            innerColumn.GetValueAt(offset, dataValueContainer, child);
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            throw new NotSupportedException("Column with offset does not support InsertAt.");
        }

        public void InsertRangeFrom(int index, IColumn otherColumn, int start, int count)
        {
            throw new NotSupportedException("Column with offset does not support InsertRangeFrom.");
        }

        public void RemoveAt(in int index)
        {
            throw new NotSupportedException("Column with offset does not support RemoveAt.");
        }

        public void RemoveRange(in int index, in int count)
        {
            throw new NotSupportedException("Column with offset does not support RemoveRange.");
        }

        public void Rent(int count)
        {
            offsets.Rent(count);
            innerColumn.Rent(count);
        }

        public void Return()
        {
            offsets.Return();
            innerColumn.Return();
        }

        public (int, int) SearchBoundries<T>(in T value, in int start, in int end, in ReferenceSegment? child, bool desc = false) where T : IDataValue
        {
            throw new NotSupportedException("Column with offset does not SearchBoundries.");
        }

        public (IArrowArray, Apache.Arrow.Types.IArrowType) ToArrowArray()
        {
            throw new NotImplementedException();
        }

        public void UpdateAt<T>(in int index, in T value) where T : IDataValue
        {
            throw new NotSupportedException("Column with offset does not support UpdateAt.");
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            var offset = offsets[index];

            if (offset == NullValueIndex)
            {
                writer.WriteNullValue();
            }
            else
            {
                innerColumn.WriteToJson(in writer, offset);
            }
        }

        public Column Copy(IMemoryAllocator memoryAllocator)
        {
            throw new NotSupportedException();
        }

        public void AddToHash(in int index, ReferenceSegment? child, NonCryptographicHashAlgorithm hashAlgorithm)
        {
            var offset = offsets[index];
            if (offset == NullValueIndex)
            {
                hashAlgorithm.Append(ByteArrayUtils.nullBytes);
                return;
            }
            innerColumn.AddToHash(offset, child, hashAlgorithm);
        }

        int IColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            throw new NotImplementedException();
        }

        public SerializationEstimation GetSerializationEstimate()
        {
            throw new NotSupportedException();
        }

        void IColumn.AddFieldNodes(ref ArrowSerializer arrowSerializer)
        {
            throw new NotSupportedException();
        }

        void IColumn.AddBuffers(ref ArrowSerializer arrowSerializer)
        {
            throw new NotSupportedException();
        }

        void IColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            throw new NotSupportedException();
        }

        public static ColumnWithOffset CreateFlattened(
            IColumn column, 
            PrimitiveList<int> offsets,
            IMemoryAllocator memoryAllocator,
            out bool usedOffset)
        {
            if (column is ColumnWithOffset columnWithOffset)
            {
                PrimitiveList<int> newOffsets = new PrimitiveList<int>(memoryAllocator);
                var otherOffsets = columnWithOffset.offsets;
                for (int i = 0; i < offsets.Count; i++)
                {
                    var offset = offsets.Get(i);
                    if (offset == NullValueIndex)
                    {
                        newOffsets.Add(NullValueIndex);
                    }
                    else
                    {
                        var otherOffset = otherOffsets.Get(offset);
                        if (otherOffset == NullValueIndex)
                        {
                            newOffsets.Add(NullValueIndex);
                        }
                        else
                        {
                            newOffsets.Add(otherOffset);
                        }
                    }
                }
                usedOffset = false;
                return new ColumnWithOffset(columnWithOffset.innerColumn, newOffsets);
            }
            usedOffset = true;
            return new ColumnWithOffset(column, offsets);
        }

        public void InsertFrom(IColumn column, ref readonly ReadOnlySpan<int> sortedLookup, ref readonly ReadOnlySpan<int> insertPositions, in int lookupNullIndex)
        {
            throw new NotSupportedException("Column with offset does not support InsertFrom.");
        }

        public void DeleteBatch(ReadOnlySpan<int> targets)
        {
            throw new NotSupportedException("Column with offset does not support DeleteBatch.");
        }

        public ColumnSizeInfo GetColumnSizeInfo()
        {
            // Give the inner size, this is not an exact number, but it gives a good estimate of the size of the data,
            // without needing to calculate the exact size of the offsets which is more expensive.
            return innerColumn.GetColumnSizeInfo();
        }

        bool IColumn.SupportSelfCompareExpression => innerColumn.SupportSelfCompareExpression;

        public CompareColumnState GetColumnState()
        {
            if (innerColumn is Column c)
            {
                var state = c.GetColumnState();
                state |= CompareColumnState.IsIndirectView;
                return state;
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        public unsafe void SetSelfComparePointers(ref SelfComparePointers selfComparePointers)
        {
            if (innerColumn is Column c)
            {
                c.SetSelfComparePointers(ref selfComparePointers);
                selfComparePointers.columnOffsetsPointer = offsets.GetPointer_Unsafe();
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        public System.Linq.Expressions.Expression CreateSelfCompareExpression(
            System.Linq.Expressions.Expression selfComparePointerExpression,
            System.Linq.Expressions.Expression xExpression,
            System.Linq.Expressions.Expression yExpression)
        {
            if (innerColumn is Column c)
            {
                if (c.Type == ArrowTypeId.Null)
                {
                    // We will always get null for all offsets
                    return System.Linq.Expressions.Expression.Constant(0);
                }

                var getXOffset = NativeSortHelpers.CallGetColumnOffset(selfComparePointerExpression, xExpression);
                var getYOffset = NativeSortHelpers.CallGetColumnOffset(selfComparePointerExpression, yExpression);

                var xOffset = System.Linq.Expressions.Expression.Variable(typeof(int), "xOffset");
                var yOffset = System.Linq.Expressions.Expression.Variable(typeof(int), "yOffset");

                // Assign
                var assignXOffset = System.Linq.Expressions.Expression.Assign(xOffset, getXOffset);
                var assignYOffset = System.Linq.Expressions.Expression.Assign(yOffset, getYOffset);

                var innerCompare = innerColumn.CreateSelfCompareExpression(selfComparePointerExpression, xOffset, yOffset);

                if (c.NullCounter > 0)
                {
                    return System.Linq.Expressions.Expression.Block(
                        [xOffset, yOffset],
                        assignXOffset,
                        assignYOffset,
                        innerCompare
                        );
                }
                else
                {
                    var offsetCompare = NativeSortHelpers.CallCompareOffsets(xOffset, yOffset);
                    var offsetCompareVar = System.Linq.Expressions.Expression.Variable(typeof(int), "offsetCompare");
                    var assignOffsetCompare = System.Linq.Expressions.Expression.Assign(offsetCompareVar, offsetCompare);
                    var condition = System.Linq.Expressions.Expression.Condition(
                        System.Linq.Expressions.Expression.Equal(offsetCompareVar, System.Linq.Expressions.Expression.Constant(2)),
                        innerCompare,
                        offsetCompareVar
                        );

                    return System.Linq.Expressions.Expression.Block(
                        [xOffset, yOffset, offsetCompareVar],
                        assignXOffset,
                        assignYOffset,
                        assignOffsetCompare,
                        condition
                        );
                }
            }
            else
            {
                throw new NotSupportedException();
            }
        }
    }
}

