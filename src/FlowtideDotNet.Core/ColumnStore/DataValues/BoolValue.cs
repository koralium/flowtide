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
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Core.Flexbuffer;
using System;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    public struct BoolValue : IDataValue
    {
        public static readonly BoolValue True = new BoolValue(true);
        public static readonly BoolValue False = new BoolValue(false);

        internal bool value;

        public BoolValue(bool value)
        {
            this.value = value;
        }

        public ArrowTypeId Type => ArrowTypeId.Boolean;

        public long AsLong => throw new NotImplementedException();

        public StringValue AsString => throw new NotImplementedException();

        public bool AsBool => value;

        public double AsDouble => throw new NotImplementedException();

        public IListValue AsList => throw new NotImplementedException();

        public ReadOnlySpan<byte> AsBinary => throw new NotImplementedException();

        public IMapValue AsMap => throw new NotImplementedException();

        public decimal AsDecimal => throw new NotImplementedException();

        public bool IsNull => false;

        public TimestampTzValue AsTimestamp => throw new NotImplementedException();

        public IStructValue AsStruct => throw new NotSupportedException();

        public void Accept(in DataValueVisitor visitor)
        {
            visitor.VisitBoolValue(in this);
        }

        public void AddToHash(NonCryptographicHashAlgorithm hashAlgorithm)
        {
            if (value)
            {
                hashAlgorithm.Append(ByteArrayUtils.trueBytes);
            }
            else
            {
                hashAlgorithm.Append(ByteArrayUtils.nullBytes);
            }
        }

        public void CopyToContainer(DataValueContainer container)
        {
            container._type = ArrowTypeId.Boolean;
            container._boolValue = this;
        }

        public override string ToString()
        {
            return value ? "true" : "false";
        }


    }
}
