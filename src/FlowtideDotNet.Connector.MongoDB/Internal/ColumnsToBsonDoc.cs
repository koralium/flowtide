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

namespace FlowtideDotNet.Connector.MongoDB.Internal
{
    internal class ColumnsToBsonDoc
    {
        private readonly List<string> m_names;
        private DataValueContainer m_dataValueContainer;

        public ColumnsToBsonDoc(List<string> names)
        {
            m_names = names;
            m_dataValueContainer = new DataValueContainer();
        }

        public BsonDocument ToBson(in EventBatchData batch, in int index)
        {
            var doc = new BsonDocument();

            for (int i = 0; i < m_names.Count; i++)
            {
                batch.Columns[i].GetValueAt(index, m_dataValueContainer, default);
                doc.Add(m_names[i], ValueToBson(m_dataValueContainer));
            }

            return doc;
        }

        public BsonValue ColumnIndexToBson(IColumn column, int index)
        {
            column.GetValueAt(index, m_dataValueContainer, default);
            return ValueToBson(m_dataValueContainer);
        }

        public static BsonValue ValueToBson<T>(T value)
            where T : IDataValue
        {
            switch (value.Type)
            {
                case ArrowTypeId.Null:
                    return BsonNull.Value;
                case ArrowTypeId.Boolean:
                    return new BsonBoolean(value.AsBool);
                case ArrowTypeId.Int64:
                    return new BsonInt64(value.AsLong);
                case ArrowTypeId.Double:
                    return new BsonDouble(value.AsDouble);
                case ArrowTypeId.String:
                    return new BsonString(value.AsString.ToString());
                case ArrowTypeId.Binary:
                    return new BsonBinaryData(value.AsBinary.ToArray());
                case ArrowTypeId.Decimal128:
                    return new BsonDecimal128(value.AsDecimal);
                case ArrowTypeId.Timestamp:
                    return new BsonDateTime(value.AsTimestamp.ToDateTimeOffset().DateTime);
                case ArrowTypeId.List:
                    return ToBsonArray(value);
                case ArrowTypeId.Map:
                    return ToBsonDocument(value.AsMap);
                case ArrowTypeId.Struct:
                    return ToBsonDocumentStruct(value.AsStruct);
                default:
                    throw new NotImplementedException(value.Type.ToString());
            }
        }

        private static BsonValue ToBsonArray<T>(T value)
            where T : IDataValue
        {
            var arr = new BsonArray();
            var list = value.AsList;
            for (int i = 0; i < list.Count; i++)
            {
                arr.Add(ValueToBson(list.GetAt(i)));
            }
            return arr;
        }

        private static BsonValue ToBsonDocument<T>(T map)
            where T : IMapValue
        {
            BsonDocument doc = new BsonDocument();
            foreach (var kv in map)
            {
                if (kv.Key.Type != ArrowTypeId.String)
                {
                    throw new InvalidOperationException("Map key must be a string for mongodb");
                }
                doc.Add(kv.Key.AsString.ToString(), ValueToBson(kv.Value));
            }
            return doc;
        }

        private static BsonValue ToBsonDocumentStruct<T>(T map)
            where T : IStructValue
        {
            BsonDocument doc = new BsonDocument();
            for (int i = 0; i < map.Header.Count; i++)
            {
                var key = map.Header.GetColumnName(i);
                var value = map.GetAt(i);
                doc.Add(key, ValueToBson(value));
            }
            return doc;
        }
    }
}
