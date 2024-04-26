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
using FlowtideDotNet.Core.Flexbuffer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Kafka
{
    public class FlowtideDebeziumKeyDeserializer : IFlowtideKafkaKeyDeserializer
    {
        public FlxValue Deserialize(byte[] bytes)
        {
            var jsonDocument = JsonSerializer.Deserialize<JsonDocument>(bytes);

            if (jsonDocument == null)
            {
                return FlxValue.FromBytes(FlexBuffer.Null());
            }
            if (!jsonDocument.RootElement.TryGetProperty("payload", out var payload))
            {
                return FlxValue.FromBytes(FlexBuffer.Null());
            }

            return JsonSerializerUtils.JsonElementToValue(payload);
        }
    }
}
