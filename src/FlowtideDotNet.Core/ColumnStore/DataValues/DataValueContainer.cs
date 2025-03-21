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
        internal IListValue? _listValue;
        internal IMapValue? _mapValue;
        internal TimestampTzValue _timestampValue;
        internal ArrowTypeId _type;



        public ArrowTypeId Type => _type;

        public long AsLong => _int64Value.AsLong;

        public FlxString AsString => _stringValue.AsString;

        public bool AsBool => _boolValue.AsBool;

        public double AsDouble => _doubleValue.AsDouble;

        public IListValue AsList => _listValue!.AsList;

        public ReadOnlySpan<byte> AsBinary => _binaryValue.AsBinary;

        public IMapValue AsMap => _mapValue!.AsMap;

        public decimal AsDecimal => _decimalValue.AsDecimal;

        public bool IsNull => _type == ArrowTypeId.Null;

        public TimestampTzValue AsTimestamp => _timestampValue;

        public void Accept(in DataValueVisitor visitor)
        {
            switch (Type)
            {
                case ArrowTypeId.Null:
                    visitor.VisitNullValue(in NullValue.Instance);
                    break;
                case ArrowTypeId.Int64:
                    visitor.VisitInt64Value(in _int64Value);
                    break;
                case ArrowTypeId.Double:
                    visitor.VisitDoubleValue(in _doubleValue);
                    break;
                case ArrowTypeId.String:
                    visitor.VisitStringValue(in _stringValue);
                    break;
                case ArrowTypeId.Binary:
                    visitor.VisitBinaryValue(in _binaryValue);
                    break;
                case ArrowTypeId.Boolean:
                    visitor.VisitBoolValue(in _boolValue);
                    break;
                case ArrowTypeId.Decimal128:
                    visitor.VisitDecimalValue(in _decimalValue);
                    break;
                case ArrowTypeId.Timestamp:
                    visitor.VisitTimestampTzValue(in _timestampValue);
                    break;
                case ArrowTypeId.List:
                    if (_listValue is ListValue listVal)
                    {
                        visitor.VisitListValue(in listVal);
                    }
                    else if (_listValue is ReferenceListValue refListVal)
                    {
                        visitor.VisitReferenceListValue(in refListVal);
                    }
                    else
                    {
                        throw new System.InvalidOperationException($"Unknown list type: {_listValue!.GetType()}");
                    }
                    break;
                case ArrowTypeId.Map:
                    if (_mapValue is MapValue mapVal)
                    {
                        visitor.VisitMapValue(in mapVal);
                    }
                    else if (_mapValue is ReferenceMapValue refMapVal)
                    {
                        visitor.VisitReferenceMapValue(in refMapVal);
                    }
                    else
                    {
                        throw new System.InvalidOperationException($"Unknown map type: {_mapValue!.GetType()}");
                    }
                    break;
                default:
                    throw new System.InvalidOperationException($"Unknown type: {Type}");
            }
        }

        public void CopyToContainer(DataValueContainer container)
        {
            container._type = _type;
            container._stringValue = _stringValue;
            container._binaryValue = _binaryValue;
            container._int64Value = _int64Value;
            container._doubleValue = _doubleValue;
            container._boolValue = _boolValue;
            container._decimalValue = _decimalValue;
            container._listValue = _listValue;
            container._mapValue = _mapValue;
        }
    }
}
