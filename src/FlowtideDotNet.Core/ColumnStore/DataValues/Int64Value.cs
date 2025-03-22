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
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    // Debugger display that uses to string
    [DebuggerDisplay(@"\{Int64: {_value}\}")]
    public struct Int64Value : IDataValue
    {
        private long _value;

        public Int64Value(long value)
        {
            _value = value;
        }

        public ArrowTypeId Type => ArrowTypeId.Int64;

        public long AsLong => _value;

        public FlxString AsString => throw new NotImplementedException();

        public bool AsBool => throw new NotImplementedException();

        public double AsDouble => throw new NotImplementedException();

        public IListValue AsList => throw new NotImplementedException();

        public ReadOnlySpan<byte> AsBinary => throw new NotImplementedException();

        public IMapValue AsMap => throw new NotImplementedException();

        public decimal AsDecimal => throw new NotImplementedException();

        public bool IsNull => false;

        public TimestampTzValue AsTimestamp => throw new NotImplementedException();

        public void Accept(in DataValueVisitor visitor)
        {
            visitor.VisitInt64Value(in this);
        }

        public void AddToHash(NonCryptographicHashAlgorithm hashAlgorithm)
        {
            Span<byte> buffer = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(buffer, _value);
            hashAlgorithm.Append(buffer);
        }

        public void CopyToContainer(DataValueContainer container)
        {
            container._type = ArrowTypeId.Int64;
            container._int64Value = this;
        }

        public override string ToString()
        {
            return _value.ToString();
        }


    }
}
