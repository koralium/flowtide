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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    /// <summary>
    /// Container used to avoid boxing as much as possible.
    /// Caller can allocate this once on the heap and call GetValueAt passing in this container to avoid boxing.
    /// </summary>
    public class DataValueContainer : IDataValue
    {
        internal StringValue _stringValue;
        internal BinaryValue _binaryValue;
        internal Int64Value _int64Value;
        internal DoubleValue _doubleValue;
        internal BoolValue _boolValue;
        internal DecimalValue _decimalValue;
        internal ListValue _listValue;
        internal IMapValue? _mapValue;
        internal ArrowTypeId _type;


        public ArrowTypeId Type => _type;

        public long AsLong => _int64Value.AsLong;

        public FlxString AsString => _stringValue.AsString;

        public bool AsBool => _boolValue.AsBool;

        public double AsDouble => _doubleValue.AsDouble;

        public ListValue AsList => _listValue.AsList;

        public Span<byte> AsBinary => _binaryValue.AsBinary;

        public IMapValue AsMap => _mapValue!.AsMap;

        public decimal AsDecimal => _decimalValue.AsDecimal;
    }
}
