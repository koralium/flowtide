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

using Confluent.Kafka;
using FlowtideDotNet.Core.Lineage;

namespace FlowtideDotNet.OpenLineage.Kafka.Internal
{
    internal class OpenLineageKafkaTransport : IOpenLineageTransport
    {
        private readonly IProducer<string, string> _producer;
        private readonly string _topicName;
        private readonly string? _messageKey;

        public OpenLineageKafkaTransport(OpenLineageKafkaOptions options)
        {
            if (options.ProducerConfig == null)
            {
                throw new ArgumentException("OpenLineageKafkaOptions.ProducerConfig must be set");
            }
            if (string.IsNullOrWhiteSpace(options.TopicName))
            {
                throw new ArgumentException("OpenLineageKafkaOptions.TopicName must be set");
            }
            _topicName = options.TopicName;
            _messageKey = options.MessageKey;
            _producer = new ProducerBuilder<string, string>(options.ProducerConfig).Build();
        }

        public Task EmitAsync(OpenLineageTransportEvent lineageEvent, CancellationToken cancellationToken)
        {
            return _producer.ProduceAsync(_topicName, new Message<string, string>()
            {
                // Default key of the OpenLineage Kafka transport for run events
                Key = _messageKey ?? $"run:{lineageEvent.JobNamespace}/{lineageEvent.JobName}",
                Value = lineageEvent.Json
            }, cancellationToken);
        }

        public void Dispose()
        {
            _producer.Dispose();
        }
    }
}
