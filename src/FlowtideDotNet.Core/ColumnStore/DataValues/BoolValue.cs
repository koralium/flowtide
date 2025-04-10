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
using System.Text.Json;

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

        public FlxString AsString => throw new NotImplementedException();

        public bool AsBool => value;

        public double AsDouble => throw new NotImplementedException();

        public IListValue AsList => throw new NotImplementedException();

        public ReadOnlySpan<byte> AsBinary => throw new NotImplementedException();

        public IMapValue AsMap => throw new NotImplementedException();

        public decimal AsDecimal => throw new NotImplementedException();

        public bool IsNull => false;

        public TimestampTzValue AsTimestamp => throw new NotImplementedException();

        public IStructValue AsStructValue => throw new NotImplementedException();

        public void Accept(in DataValueVisitor visitor)
        {
            visitor.VisitBoolValue(in this);
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
