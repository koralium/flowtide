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
using System.Diagnostics;

namespace FlowtideDotNet.Core.ColumnStore.DataValues
{
    [DebuggerDisplay(@"\{null\}")]
    public struct NullValue : IDataValue
    {
        public static readonly NullValue Instance = new NullValue();
        public ArrowTypeId Type => ArrowTypeId.Null;

        public long AsLong => throw new NotImplementedException();

        public FlxString AsString => throw new NotImplementedException();

        public bool AsBool => throw new NotImplementedException();

        public double AsDouble => throw new NotImplementedException();

        public IListValue AsList => throw new NotImplementedException();

        public ReadOnlySpan<byte> AsBinary => throw new NotImplementedException();

        public IMapValue AsMap => throw new NotImplementedException();

        public decimal AsDecimal => throw new NotImplementedException();

        public bool IsNull => true;

        public TimestampTzValue AsTimestamp => throw new NotImplementedException();

        public IStructValue AsStruct => throw new NotSupportedException();

        public void Accept(in DataValueVisitor visitor)
        {
            visitor.VisitNullValue(in this);
        }

        public void CopyToContainer(DataValueContainer container)
        {
            container._type = ArrowTypeId.Null;
        }

        public override string ToString()
        {
            return "null";
        }
    }
}
