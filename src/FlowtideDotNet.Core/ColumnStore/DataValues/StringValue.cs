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
using System.Diagnostics;
using System.Text;

namespace FlowtideDotNet.Core.ColumnStore
{
    [DebuggerDisplay(@"\{String: {ToString()}\}")]
    public struct StringValue : IDataValue
    {
        private ReadOnlyMemory<byte> _utf8;

        public ArrowTypeId Type => ArrowTypeId.String;

        public long AsLong => throw new NotImplementedException();

        public FlxString AsString => new FlxString(_utf8.Span);

        public bool AsBool => throw new NotImplementedException();

        public double AsDouble => throw new NotImplementedException();

        public IListValue AsList => throw new NotImplementedException();

        public ReadOnlySpan<byte> AsBinary => _utf8.Span;

        public IMapValue AsMap => throw new NotImplementedException();

        public decimal AsDecimal => throw new NotImplementedException();

        public bool IsNull => false;

        public TimestampTzValue AsTimestamp => throw new NotImplementedException();

        public IStructValue AsStruct => throw new NotSupportedException();

        public StringValue(byte[] utf8)
        {
            _utf8 = utf8;
        }

        public StringValue(ReadOnlyMemory<byte> utf8)
        {
            _utf8 = utf8;
        }

        public StringValue(string value)
        {
            _utf8 = Encoding.UTF8.GetBytes(value);
        }

        public override string ToString()
        {
            return Encoding.UTF8.GetString(_utf8.Span);
        }

        public void CopyToContainer(DataValueContainer container)
        {
            container._type = ArrowTypeId.String;
            container._stringValue = this;
        }

        public void Accept(in DataValueVisitor visitor)
        {
            visitor.VisitStringValue(in this);
        }
    }
}
