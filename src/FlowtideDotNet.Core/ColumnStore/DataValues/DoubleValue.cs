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
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    public struct DoubleValue : IDataValue
    {
        public DoubleValue(double value)
        {
            AsDouble = value;
        }


        public double AsDouble { get; }

        public ArrowTypeId Type => ArrowTypeId.Double;

        public long AsLong => throw new NotImplementedException();

        public StringValue AsString => throw new NotImplementedException();

        public bool AsBool => throw new NotImplementedException();

        public IListValue AsList => throw new NotImplementedException();

        public ReadOnlySpan<byte> AsBinary => throw new NotImplementedException();

        public IMapValue AsMap => throw new NotImplementedException();

        public decimal AsDecimal => throw new NotImplementedException();

        public bool IsNull => false;

        public TimestampTzValue AsTimestamp => throw new NotImplementedException();

        public IStructValue AsStruct => throw new NotSupportedException();

        public void Accept(in DataValueVisitor visitor)
        {
            visitor.VisitDoubleValue(in this);
        }

        public void AddToHash(NonCryptographicHashAlgorithm hashAlgorithm)
        {
            Span<byte> buffer = stackalloc byte[8];
            BinaryPrimitives.WriteDoubleLittleEndian(buffer, AsDouble);
            hashAlgorithm.Append(buffer);
        }

        public int CompareTo(in IDataValue other)
        {
            return AsDouble.CompareTo(other.AsDouble);
        }

        public void CopyToContainer(DataValueContainer container)
        {
            container._type = ArrowTypeId.Double;
            container._doubleValue = this;
        }

        public override string ToString()
        {
            return AsDouble.ToString();
        }

    }
}
