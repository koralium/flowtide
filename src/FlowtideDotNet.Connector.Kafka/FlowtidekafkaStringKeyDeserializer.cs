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
using System.Text;

namespace FlowtideDotNet.Connector.Kafka
{
    public class FlowtidekafkaStringKeyDeserializer : IFlowtideKafkaKeyDeserializer
    {
        public FlxValue Deserialize(byte[] bytes)
        {
            return FlxValue.FromMemory(FlexBuffer.SingleValue(Encoding.UTF8.GetString(bytes)));
        }
    }
}
