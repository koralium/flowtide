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
using FlowtideDotNet.Core.ColumnStore;
using System.Globalization;
using System.Text;

namespace FlowtideDotNet.Connector.Kafka
{
    public class FlowtideKafkaStringKeySerializer : IFlowtideKafkaKeySerializer
    {
        public byte[] Serialize(FlxValue key)
        {
            return key.AsFlxString.Span.ToArray();
        }

        public byte[] Serialize<T>(T key) where T : IDataValue
        {
            return Encoding.UTF8.GetBytes(ColumnToString(key));
        }

        private static string ColumnToString<T>(T dataValue)
            where T : IDataValue
        {
            switch (dataValue.Type)
            {
                case ArrowTypeId.Boolean:
                    return dataValue.AsBool.ToString();
                case ArrowTypeId.Null:
                    return "null";
                case ArrowTypeId.Decimal128:
                    return dataValue.AsDecimal.ToString(CultureInfo.InvariantCulture);
                case ArrowTypeId.Double:
                    return dataValue.AsDouble.ToString(CultureInfo.InvariantCulture);
                case ArrowTypeId.Map:
                    return dataValue.AsMap.ToString()!;
                case ArrowTypeId.Int64:
                    return dataValue.AsLong.ToString(CultureInfo.InvariantCulture);
                case ArrowTypeId.String:
                    return dataValue.AsString.ToString();
                default:
                    throw new InvalidOperationException($"Unsupported type {dataValue.Type}");
            }
        }
    }
}
