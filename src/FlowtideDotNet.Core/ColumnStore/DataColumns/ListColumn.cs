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

using Apache.Arrow.Types;
using Apache.Arrow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Substrait.Expressions;
using System.Buffers;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using static Substrait.Protobuf.Expression.Types.Literal.Types;
using System.Collections;
using static SqlParser.Ast.TableConstraint;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.ColumnStore
{
    public class ListColumn : IDataColumn, IEnumerable<IEnumerable<IDataValue>>
    {
        private readonly Column _internalColumn;
        private readonly IntList _offsets;
        private bool disposedValue;

        public int Count => _offsets.Count - 1;

        public ArrowTypeId Type => ArrowTypeId.List;

        public ListColumn(IMemoryAllocator memoryAllocator)
        {
            _internalColumn = Column.Create(memoryAllocator);
            _offsets = new IntList(memoryAllocator);
            _offsets.Add(0);
        }

        public ListColumn(Column internalColumn, IMemoryOwner<byte> offsetMemory, int offsetCount, IMemoryAllocator memoryAllocator)
        {
            _offsets = new IntList(offsetMemory, offsetCount, memoryAllocator);
            _internalColumn = internalColumn;
        }

        public int Add(in IDataValue value)
        {
            var list = value.AsList;

            var currentOffset = Count;
            var listLength = list.Count;
            for (int i = 0; i < listLength; i++)
            {
                _internalColumn.Add(list.GetAt(i));
            }
            _offsets.Add(_internalColumn.Count);

            return currentOffset;
        }

        public int Add<T>(in T value) where T : IDataValue
        {
            var currentOffset = Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _offsets.Add(_internalColumn.Count);
                return currentOffset;
            }

            var list = value.AsList;
            var listLength = list.Count;
            for (int i = 0; i < listLength; i++)
            {
                _internalColumn.Add(list.GetAt(i));
            }
            _offsets.Add(_internalColumn.Count);

            return currentOffset;
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            throw new NotImplementedException();
        }

        public int CompareTo<T>(in int index, in T value, in ReferenceSegment? child, in BitmapList? validityList) where T : IDataValue
        {
            if (validityList != null &&
                !validityList.Get(index))
            {
                if (value.Type == ArrowTypeId.Null)
                {
                    return 0;
                }
                return -1;
            }
            else if (value.Type == ArrowTypeId.Null)
            {
                return 1;
            }
            var otherList = value.AsList;
            
            var startOffset = _offsets.Get(index);
            var endOffset = _offsets.Get(index + 1);
            var thisListCount = endOffset - startOffset;
            var lengthCompare = thisListCount.CompareTo(otherList.Count);
            if (lengthCompare != 0)
            {
                return lengthCompare;
            }
            for (int i = 0; i < thisListCount; i++)
            {
                var otherValue = otherList.GetAt(i);
                var compare = _internalColumn.CompareTo(startOffset + i, otherValue, null);
                if (compare != 0)
                {
                    return compare;
                }
            }

            return 0;
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            return new ReferenceListValue(_internalColumn, _offsets.Get(index), _offsets.Get(index + 1));
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            dataValueContainer._listValue = new ReferenceListValue(_internalColumn, _offsets.Get(index), _offsets.Get(index + 1));
            dataValueContainer._type = ArrowTypeId.List;
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc) 
            where T : IDataValue
        {
            if (desc)
            {
                return BoundarySearch.SearchBoundriesForDataColumnDesc(this, in dataValue, start, end, child, default);
            }
            return BoundarySearch.SearchBoundriesForDataColumn(this, in dataValue, start, end, child, default);
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            var currentStart = _offsets.Get(index);
            var currentEnd = _offsets.Get(index + 1);

            if (value.Type == ArrowTypeId.Null)
            {
                for (int i = currentEnd - 1; i >= currentStart; i--)
                {
                    _internalColumn.RemoveAt(currentStart);
                }
                _offsets.Update(index + 1, currentStart, currentStart - currentEnd);
                return index;
            }

            var list = value.AsList;
            var listLength = list.Count;
            var currentLength = currentEnd - currentStart;
            if (listLength <= currentLength)
            {
                for (int i = 0; i < listLength; i++)
                {
                    _internalColumn.UpdateAt(currentStart + i, list.GetAt(i));
                }

                if (currentLength > listLength)
                {
                    for (int i = currentLength - 1; i >= listLength; i--)
                    {
                        _internalColumn.RemoveAt(currentStart + i);
                    }
                    _offsets.Update(index + 1, currentStart + listLength, listLength - currentLength);
                }
                
            }
            else
            {
                // New list is larger than the current list

                // Update existing elements
                for (int i = 0; i < currentLength; i++)
                {
                    _internalColumn.UpdateAt(currentStart + i, list.GetAt(i));
                }

                // Insert new elements
                for (int i = 0; i < (listLength - currentLength); i++)
                {
                    _internalColumn.InsertAt(currentStart + currentLength + i, list.GetAt(currentLength + i));
                }

                // Update offset
                _offsets.Update(index + 1, currentStart + listLength, listLength - currentLength);
            }
            return index;
        }

        public void RemoveAt(in int index)
        {
            var startOffset = _offsets.Get(index);
            var endOffset = _offsets.Get(index + 1);
            for (int i = endOffset - 1; i >= startOffset; i--)
            {
                _internalColumn.RemoveAt(i);
            }
            _offsets.RemoveAt(index + 1, startOffset - endOffset);
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                var endOffset = _offsets.Get(index);
                _offsets.InsertAt(index, endOffset);
                return;
            }
            var list = value.AsList;

            var startOffset = _offsets.Get(index);

            for (int i = 0; i < list.Count; i++)
            {
                _internalColumn.InsertAt(startOffset + i, list.GetAt(i));
            }

            _offsets.InsertAt(index + 1, startOffset + list.Count, list.Count);
        }

        public (IArrowArray, IArrowType) ToArrowArray(Apache.Arrow.ArrowBuffer nullBuffer, int nullCount)
        {
            var (arr, type) = _internalColumn.ToArrowArray();
            var customMetadata = EventArrowSerializer.GetCustomMetadata(type);
            var field = new Field("item", type, true, customMetadata);
            var listType = new ListType(field);
            var offsetBuffer = new ArrowBuffer(_offsets.Memory);
            return (new Apache.Arrow.ListArray(listType, Count, offsetBuffer, arr, nullBuffer, nullCount), listType);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _internalColumn.Dispose();
                    _offsets.Dispose();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            return ArrowTypeId.List;
        }

        public void Clear()
        {
            _offsets.Clear();
            _offsets.Add(0);
            _internalColumn.Clear();
        }

        /// <summary>
        /// Allows adding values to a new list directly without creating a list value type.
        /// This allows for more efficient list creation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        public void AddToNewList<T>(in T value) where T : IDataValue
        {
            _internalColumn.Add(value);
        }

        /// <summary>
        /// Signals an end to a list created with AddToNewList.
        /// </summary>
        /// <returns></returns>
        public int EndNewList()
        {
            var currentOffset = _offsets.Count - 1;
            _offsets.Add(_internalColumn.Count);
            return currentOffset;
        }

        private IEnumerable<IDataValue> GetListValues(int index)
        {
            var startOffset = _offsets.Get(index);
            var endOffset = _offsets.Get(index + 1);

            for (int i = startOffset; i < endOffset; i++)
            {
                yield return _internalColumn.GetValueAt(i, default);
            }
        }

        private IEnumerable<IEnumerable<IDataValue>> GetEnumerable()
        {
            for (int i = 0; i < Count; i++)
            {
                yield return GetListValues(i);
            }
        }

        public IEnumerator<IEnumerable<IDataValue>> GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        public int GetByteSize(int start, int end)
        {
            var startOffset = _offsets.Get(start);
            var endOffset = _offsets.Get(end + 1);
            return _internalColumn.GetByteSize(startOffset, endOffset) + ((end - start + 1) * sizeof(int));
        }

        public int GetByteSize()
        {
            return _internalColumn.GetByteSize() + (_offsets.Count * sizeof(int));
        }
    }
}
