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
            if (bsonDocument.TryGetValue(propertyName, out var val))
            {
                BsonValueToColumn(ref column, ref val);
            }
            else
            {
                column.Add(NullValue.Instance);
            }
        }

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
                    column.Add(new Int64Value(bsonValue.ToUniversalTime().Ticks));
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
                default:
                    throw new NotImplementedException($"Unknown BSON type {bsonValue.BsonType}");
            }
        }
    }
}
