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
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Core.ColumnStore
{
    public struct BinaryValue : IDataValue
    {
        private readonly ReadOnlyMemory<byte> _bytes;

        public BinaryValue(ReadOnlyMemory<byte> bytes)
        {
            this._bytes = bytes;
        }
        public ArrowTypeId Type => ArrowTypeId.Binary;

        public long AsLong => throw new NotImplementedException();

        public StringValue AsString => throw new NotImplementedException();

        public bool AsBool => throw new NotImplementedException();

        public double AsDouble => throw new NotImplementedException();

        public IListValue AsList => throw new NotImplementedException();

        public ReadOnlySpan<byte> AsBinary => _bytes.Span;

        public IMapValue AsMap => throw new NotImplementedException();

        public decimal AsDecimal => throw new NotImplementedException();

        public bool IsNull => false;

        public TimestampTzValue AsTimestamp => throw new NotImplementedException();

        public IStructValue AsStruct => throw new NotSupportedException();

        public void Accept(in DataValueVisitor visitor)
        {
            visitor.VisitBinaryValue(in this);
        }

        public void CopyToContainer(DataValueContainer container)
        {
            container._type = ArrowTypeId.Binary;
            container._binaryValue = this;
        }

        public override string ToString()
        {
            StringBuilder builder = new StringBuilder();
            builder.Append("[");
            for (int i = 0; i < _bytes.Length; i++)
            {
                builder.Append(_bytes.Span[i].ToString());

                if (i < _bytes.Length - 1)
                {
                    builder.Append(", ");
                }
            }

            builder.Append("]");
            return builder.ToString();
        }


    }
}
