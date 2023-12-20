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
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Kafka
{
    public interface IFlowtideKafkaDeserializer
    {
        /// <summary>
        /// Initialize the deserializer
        /// </summary>
        /// <param name="readRelation">Read relation that contains table schema</param>
        /// <returns></returns>
        Task Initialize(ReadRelation readRelation);

        /// <summary>
        /// Deserializes the value bytes into a stream event.
        /// The deserializer must determine if its a delete or insert and return the appropriate stream event.
        /// </summary>
        /// <param name="keyDeserializer"></param>
        /// <param name="valueBytes"></param>
        /// <param name="keyBytes"></param>
        /// <returns></returns>
        StreamEvent Deserialize(IFlowtideKafkaKeyDeserializer keyDeserializer, byte[]? valueBytes, byte[]? keyBytes);
    }
}
