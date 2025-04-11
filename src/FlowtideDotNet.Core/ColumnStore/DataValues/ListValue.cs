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

using FlowtideDotNet.Core.Flexbuffer;

namespace FlowtideDotNet.Core.ColumnStore.DataValues
{
    public struct ListValue : IListValue
    {
        public static readonly ListValue Empty = new ListValue();

        private readonly IReadOnlyList<IDataValue> dataValues;

        public ListValue(IReadOnlyList<IDataValue> dataValues)
        {
            this.dataValues = dataValues;
        }

        public ListValue(params IDataValue[] dataValues)
        {
            this.dataValues = dataValues;
        }

        public int Count => dataValues.Count;

        public ArrowTypeId Type => ArrowTypeId.List;

        public long AsLong => throw new NotImplementedException();

        public FlxString AsString => throw new NotImplementedException();

        public bool AsBool => throw new NotImplementedException();

        public double AsDouble => throw new NotImplementedException();

        public IListValue AsList => this;

        public ReadOnlySpan<byte> AsBinary => throw new NotImplementedException();

        public IMapValue AsMap => throw new NotImplementedException();

        public decimal AsDecimal => throw new NotImplementedException();

        public bool IsNull => false;

        public TimestampTzValue AsTimestamp => throw new NotImplementedException();

        public IStructValue AsStruct => throw new NotImplementedException();

        public void Accept(in DataValueVisitor visitor)
        {
            visitor.VisitListValue(in this);
        }

        public void CopyToContainer(DataValueContainer container)
        {
            container._type = ArrowTypeId.List;
            container._listValue = this;
        }

        public IDataValue GetAt(in int index)
        {
            return dataValues[index];
        }
    }
}
