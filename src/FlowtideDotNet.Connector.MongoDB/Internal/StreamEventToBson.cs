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

using FlexBuffers;
using FlowtideDotNet.Core;
using MongoDB.Bson;

namespace FlowtideDotNet.Connector.MongoDB.Internal
{
    internal class StreamEventToBson
    {
        private readonly List<string> names;

        public StreamEventToBson(List<string> names)
        {
            this.names = names;
        }

        public BsonDocument ToBson(in StreamEvent streamEvent)
        {
            var doc = new BsonDocument();
            
            for (int i = 0; i < names.Count; i++)
            {
                doc.Add(names[i], ToBsonValue(streamEvent.GetColumn(i)));
            }
            
            return doc;
        }

        public static BsonValue ToBsonValue(FlxValue flxValue)
        {
            switch (flxValue.ValueType)
            {
                case FlexBuffers.Type.Null:
                    return BsonNull.Value;
                case FlexBuffers.Type.Bool:
                    return new BsonBoolean(flxValue.AsBool);
                case FlexBuffers.Type.Int:
                    return new BsonInt64(flxValue.AsLong);
                case FlexBuffers.Type.Uint:
                    return new BsonInt64((long)flxValue.AsULong);
                case FlexBuffers.Type.Float:
                    return new BsonDouble((long)flxValue.AsULong);
                case FlexBuffers.Type.String:
                    return new BsonString(flxValue.AsString);
                case FlexBuffers.Type.Key:
                    return new BsonString(flxValue.AsString);
                case FlexBuffers.Type.Blob:
                    return new BsonBinaryData(flxValue.AsBlob.ToArray());
                case FlexBuffers.Type.Decimal:
                    return new BsonDecimal128(flxValue.AsDecimal);
                case FlexBuffers.Type.Vector:
                    return ToBsonArray(flxValue.AsVector);
                case FlexBuffers.Type.Map:
                    return ToBsonDocument(flxValue.AsMap);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private static BsonValue ToBsonArray(FlxVector values)
        {
            var arr = new BsonArray();
            for (int i = 0; i < values.Length; i++)
            {
                arr.Add(ToBsonValue(values[i]));
            }
            return arr;
        }

        private static BsonValue ToBsonDocument(FlxMap map)
        {
            BsonDocument doc = new BsonDocument();
            foreach (var kv in map)
            {
                doc.Add(kv.Key, ToBsonValue(kv.Value));
            }
            return doc;
        }
    }
}
