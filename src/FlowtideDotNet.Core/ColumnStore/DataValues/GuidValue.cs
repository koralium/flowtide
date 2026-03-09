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

using System;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.DataValues
{
    public struct GuidValue : IDataValue
    {
        private Guid _guid;

        public GuidValue(Guid guid)
        {
            _guid = guid;
        }

        public ArrowTypeId Type => ArrowTypeId.Guid;

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

        public IStructValue AsStruct => throw new NotImplementedException();

        public Guid AsGuid => _guid;

        public void Accept(in DataValueVisitor visitor)
        {
            visitor.VisitGuidValue(in this);
        }

        public void AddToHash(NonCryptographicHashAlgorithm hashAlgorithm)
        {
            Span<byte> buffer = stackalloc byte[16];
            _guid.TryWriteBytes(buffer);
            hashAlgorithm.Append(buffer);
        }

        public void CopyToContainer(DataValueContainer container)
        {
            container._type = ArrowTypeId.Guid;
            container._guidValue = this;
        }
    }
}
