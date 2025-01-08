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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.MongoDB.Internal
{
    internal class BsonDocToColumn
    {
        private readonly string propertyName;

        public BsonDocToColumn(string propertyName)
        {
            this.propertyName = propertyName;
        }

        public void AddToColumn(Column column, BsonDocument bsonDocument)
        {
            if (propertyName == "_doc")
            {
                column.Add(BsonMapToDataValue(bsonDocument));
            }
            else if (bsonDocument.TryGetValue(propertyName, out var val))
            {
                BsonValueToColumn(ref column, ref val);
            }
            else
            {
                column.Add(NullValue.Instance);
            }
        }

        /// <summary>
        /// Maps directly from bson into the column, this lowers the allocation on the heap compared to getting the IDataValue and then inserting.
        /// </summary>
        /// <param name="column"></param>
        /// <param name="bsonValue"></param>
        /// <exception cref="NotImplementedException"></exception>
        private void BsonValueToColumn(ref readonly Column column, ref readonly BsonValue bsonValue)
        {
            switch (bsonValue.BsonType)
            {
                case BsonType.Boolean:
                    column.Add(new BoolValue(bsonValue.AsBoolean));
                    break;
                case BsonType.String:
                    column.Add(new StringValue(bsonValue.AsString));
                    break;
                case BsonType.Int64:
                    column.Add(new Int64Value(bsonValue.AsInt64));
                    break;
                case BsonType.Binary:
                    column.Add(new BinaryValue(bsonValue.AsByteArray));
                    break;
                case BsonType.DateTime:
                    column.Add(new TimestampTzValue(bsonValue.ToUniversalTime()));
                    break;
                case BsonType.ObjectId:
                    column.Add(new StringValue(bsonValue.AsObjectId.ToString()));
                    break;
                case BsonType.Double:
                    column.Add(new DoubleValue(bsonValue.AsDouble));
                    break;
                case BsonType.Int32:
                    column.Add(new Int64Value(bsonValue.AsInt32));
                    break;
                case BsonType.Null:
                    column.Add(NullValue.Instance);
                    break;
                case BsonType.Timestamp:
                    column.Add(new Int64Value(bsonValue.AsBsonTimestamp.Timestamp));
                    break;
                case BsonType.Document:
                    column.Add(BsonMapToDataValue(bsonValue.AsBsonDocument));
                    break;
                case BsonType.Array:
                    column.Add(BsonListToDataValue(bsonValue.AsBsonArray));
                    break;
                default:
                    throw new NotImplementedException($"Unknown BSON type {bsonValue.BsonType}");
            }
        }

        private IDataValue BsonValueToDataValue(in BsonValue bsonValue)
        {
            switch (bsonValue.BsonType)
            {
                case BsonType.Boolean:
                    return new BoolValue(bsonValue.AsBoolean);
                case BsonType.String:
                    return new StringValue(bsonValue.AsString);
                case BsonType.Int64:
                    return new Int64Value(bsonValue.AsInt64);
                case BsonType.Binary:
                    return new BinaryValue(bsonValue.AsByteArray);
                case BsonType.DateTime:
                    return new TimestampTzValue(bsonValue.ToUniversalTime());
                case BsonType.ObjectId:
                    return new StringValue(bsonValue.AsObjectId.ToString());
                case BsonType.Double:
                    return new DoubleValue(bsonValue.AsDouble);
                case BsonType.Int32:
                    return new Int64Value(bsonValue.AsInt32);
                case BsonType.Null:
                    return NullValue.Instance;
                case BsonType.Timestamp:
                    return new TimestampTzValue(bsonValue.AsBsonTimestamp.Timestamp, 0);
                case BsonType.Document:
                    return BsonMapToDataValue(bsonValue.AsBsonDocument);
                case BsonType.Array:
                    return BsonListToDataValue(bsonValue.AsBsonArray);
                default:
                    throw new NotImplementedException($"Unknown BSON type {bsonValue.BsonType}");
            }
        }

        private IDataValue BsonMapToDataValue(BsonDocument bsonDocument)
        {
            List<KeyValuePair<IDataValue, IDataValue>> list = new List<KeyValuePair<IDataValue, IDataValue>>();
            foreach (var kvp in bsonDocument)
            {
                var val = kvp.Value;
                KeyValuePair<IDataValue, IDataValue> propAndValue = new KeyValuePair<IDataValue, IDataValue>(new StringValue(kvp.Name), BsonValueToDataValue(in val));
                list.Add(propAndValue);
            }

            return new MapValue(list);
        }

        private IDataValue BsonListToDataValue(BsonArray bsonArray)
        {
            List<IDataValue> list = new List<IDataValue>();
            foreach (var val in bsonArray)
            {
                list.Add(BsonValueToDataValue(val));
            }
            return new ListValue(list);
        }
    }
}
