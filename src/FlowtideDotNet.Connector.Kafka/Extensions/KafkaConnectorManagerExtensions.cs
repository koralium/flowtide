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

using FlowtideDotNet.Connector.Kafka;
using FlowtideDotNet.Connector.Kafka.Internal;
using FlowtideDotNet.Core.Operators.Write;

namespace FlowtideDotNet.Core
{
    public static class KafkaConnectorManagerExtensions
    {
        public static IConnectorManager AddKafkaSink(this IConnectorManager connectorManager, string regexPattern, FlowtideKafkaSinkOptions options, ExecutionMode executionMode = ExecutionMode.Hybrid)
        {
            connectorManager.AddSink(new KafkaSinkFactory(regexPattern, options, executionMode));
            return connectorManager;
        }

        public static IConnectorManager AddKafkaSource(this IConnectorManager connectorManager, string regexPattern, FlowtideKafkaSourceOptions options)
        {
            connectorManager.AddSource(new KafkaSourceFactory(regexPattern, options));
            return connectorManager;
        }
    }
}
