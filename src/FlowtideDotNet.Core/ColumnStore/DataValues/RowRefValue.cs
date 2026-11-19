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

using System.Buffers.Binary;
using System.Diagnostics;
using System.IO.Hashing;

namespace FlowtideDotNet.Core.ColumnStore.DataValues
{
    /// <summary>
    /// A reference to a row that is stored elsewhere.
    /// </summary>
    [DebuggerDisplay(@"\{RowRef: {_value}\}")]
    internal readonly struct RowRefValue : IDataValue, IComparable<RowRefValue>
    {
        private readonly ulong _value;

        public RowRefValue(ulong value)
        {
            _value = value;
        }

        public ulong Value => _value;

        public ArrowTypeId Type => ArrowTypeId.RowRef;

        // The reference as its 64 bits
        public long AsLong => (long)_value;

        public StringValue AsString => throw new NotSupportedException();

        public bool AsBool => throw new NotSupportedException();

        public double AsDouble => throw new NotSupportedException();

        public IListValue AsList => throw new NotSupportedException();

        public ReadOnlySpan<byte> AsBinary => throw new NotSupportedException();

        public IMapValue AsMap => throw new NotSupportedException();

        public decimal AsDecimal => throw new NotSupportedException();

        public bool IsNull => false;

        public TimestampTzValue AsTimestamp => throw new NotSupportedException();

        public IStructValue AsStruct => throw new NotSupportedException();

        public void Accept(in DataValueVisitor visitor)
        {
            visitor.VisitRowRefValue(in this);
        }

        public void AddToHash(NonCryptographicHashAlgorithm hashAlgorithm)
        {
            Span<byte> buffer = stackalloc byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(buffer, _value);
            hashAlgorithm.Append(buffer);
        }

        public void CopyToContainer(DataValueContainer container)
        {
            container._type = ArrowTypeId.RowRef;
            container._int64Value = new Int64Value((long)_value);
        }

        public int CompareTo(RowRefValue other)
        {
            return _value.CompareTo(other._value);
        }

        public override string ToString()
        {
            return _value.ToString();
        }
    }
}
