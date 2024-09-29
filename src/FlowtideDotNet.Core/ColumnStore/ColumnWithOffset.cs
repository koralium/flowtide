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
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static SqlParser.Ast.TableConstraint;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal class ColumnWithOffset : IColumn
    {
        private readonly IColumn innerColumn;
        private readonly PrimitiveList<int> offsets;
        private readonly bool includeNullValueAtEnd;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="innerColumn"></param>
        /// <param name="offsets"></param>
        /// <param name="includeNullValueAtEnd">Adds an extra index at the end which always gives out null, useful
        /// when doing left joins or similar to easily add null without needing to modify the inner data columns.
        /// Since these can be used by different operators, all data would need to be copied.</param>
        public ColumnWithOffset(IColumn innerColumn, PrimitiveList<int> offsets, bool includeNullValueAtEnd)
        {
            this.innerColumn = innerColumn;
            this.offsets = offsets;
            this.includeNullValueAtEnd = includeNullValueAtEnd;
        }

        public int Count => offsets.Count;

        public ArrowTypeId Type => innerColumn.Type;

        IDataColumn IColumn.DataColumn => innerColumn.DataColumn;

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
                size += innerColumn.GetByteSize(offset, offset);
            }
            return size;
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            var offset = offsets[index];
            if (includeNullValueAtEnd && offset == innerColumn.Count)
            {
                return ArrowTypeId.Null;
            }
            return innerColumn.GetTypeAt(offset, child);
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            var offset = offsets[index];
            if (includeNullValueAtEnd && offset == innerColumn.Count)
            {
                return NullValue.Instance;
            }
            return innerColumn.GetValueAt(offset, child);
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            var offset = offsets[index];
            if (includeNullValueAtEnd && offset == innerColumn.Count)
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
    }
}
