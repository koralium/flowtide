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

using FlowtideDotNet.Core.ColumnStore.DataColumns;
using FlowtideDotNet.Core.Flexbuffer;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.DataValues
{
    public struct ReferenceStructValue : IStructValue
    {
        internal readonly StructColumn column;
        internal readonly int index;

        internal ReferenceStructValue(StructColumn column, int index)
        {
            this.column = column;
            this.index = index;
        }
        public StructHeader Header => column._header;

        public ArrowTypeId Type => ArrowTypeId.Struct;

        public long AsLong => throw new NotImplementedException();

        public StringValue AsString => throw new NotImplementedException();

        public bool AsBool => throw new NotImplementedException();

        public double AsDouble => throw new NotImplementedException();

        public IListValue AsList => throw new NotImplementedException();

        public ReadOnlySpan<byte> AsBinary => throw new NotImplementedException();

        public IMapValue AsMap => throw new NotImplementedException();

        public decimal AsDecimal => throw new NotImplementedException();

        public bool IsNull => false;

        public TimestampTzValue AsTimestamp => throw new NotImplementedException();

        public IStructValue AsStruct => this;

        public void Accept(in DataValueVisitor visitor)
        {
            visitor.VisitReferenceStructValue(ref this);
        }

        public void CopyToContainer(DataValueContainer container)
        {
            container._structValue = this;
            container._type = ArrowTypeId.Struct;
        }

        public IDataValue GetAt(in int index)
        {
            return column._columns[index].GetValueAt(this.index, default);
        }

        public override string ToString()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("{");
            for (int i = 0; i < column._columns.Length; i++)
            {
                stringBuilder.Append(column._header.GetColumnName(i));
                stringBuilder.Append(": ");
                stringBuilder.Append(column._columns[i].GetValueAt(this.index, default).ToString());
                if (i < column._columns.Length - 1)
                {
                    stringBuilder.Append(", ");
                }
            }
            stringBuilder.Append("}");
            return stringBuilder.ToString();
        }
    }
}
