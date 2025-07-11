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
using System.IO.Hashing;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Text;

namespace FlowtideDotNet.Core.ColumnStore
{
    [DebuggerDisplay(@"\{String: {ToString()}\}")]
    public struct StringValue : IDataValue
    {
        private ReadOnlyMemory<byte> _utf8;

        public ArrowTypeId Type => ArrowTypeId.String;

        public long AsLong => throw new NotImplementedException();

        public StringValue AsString => this;

        public bool AsBool => throw new NotImplementedException();

        public double AsDouble => throw new NotImplementedException();

        public IListValue AsList => throw new NotImplementedException();

        public ReadOnlySpan<byte> AsBinary => _utf8.Span;

        public IMapValue AsMap => throw new NotImplementedException();

        public decimal AsDecimal => throw new NotImplementedException();

        public bool IsNull => false;

        public TimestampTzValue AsTimestamp => throw new NotImplementedException();

        public IStructValue AsStruct => throw new NotSupportedException();

        public ReadOnlySpan<byte> Span => _utf8.Span;

        public ReadOnlyMemory<byte> Memory => _utf8;

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

        public int CompareTo(in StringValue other)
        {
            return Compare(this, other);
        }

        public static int Compare(in StringValue v1, in StringValue v2)
        {
            return v1.Span.SequenceCompareTo(v2.Span);
        }

        public static int CompareIgnoreCase(in FlxString v1, in FlxString v2)
        {
            return Utf8Utility.CompareToOrdinalIgnoreCaseUtf8(v1.Span, v2.Span);
        }

        public void AddToHash(NonCryptographicHashAlgorithm hashAlgorithm)
        {
            hashAlgorithm.Append(_utf8.Span);
        }
    }
}
