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

using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Flexbuffer;
using System.Collections;

namespace FlowtideDotNet.Core.ColumnStore
{
    public struct ReferenceListValue : IListValue, IEnumerable<IDataValue>
    {
        internal readonly Column column;
        internal readonly int start;
        internal readonly int end;

        public ReferenceListValue(Column column, int start, int end)
        {
            this.column = column;
            this.start = start;
            this.end = end;
        }

        public ArrowTypeId Type => ArrowTypeId.List;

        public long AsLong => throw new NotImplementedException();

        public FlxString AsString => throw new NotImplementedException();

        public bool AsBool => throw new NotImplementedException();

        public double AsDouble => throw new NotImplementedException();

        public IListValue AsList => this;

        public int Count => end - start;

        public ReadOnlySpan<byte> AsBinary => throw new NotImplementedException();

        public IMapValue AsMap => throw new NotImplementedException();

        public decimal AsDecimal => throw new NotImplementedException();

        public bool IsNull => false;

        public TimestampTzValue AsTimestamp => throw new NotImplementedException();

        public IStructValue AsStructValue => throw new NotImplementedException();

        public void CopyToContainer(DataValueContainer container)
        {
            container._type = ArrowTypeId.List;
            container._listValue = this;
        }

        public IDataValue GetAt(in int index)
        {
            return column.GetValueAt(start + index, default);
        }

        private IEnumerable<IDataValue> ListValues()
        {
            for (int i = start; i < end; i++)
            {
                yield return column.GetValueAt(i, default);
            }
        }

        public IEnumerator<IDataValue> GetEnumerator()
        {
            return ListValues().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ListValues().GetEnumerator();
        }

        public override string ToString()
        {
            return $"[{String.Join(',', ListValues().Select(x => x.ToString()))}]";
        }

        public void Accept(in DataValueVisitor visitor)
        {
            visitor.VisitReferenceListValue(in this);
        }
    }
}
