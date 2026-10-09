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

using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Lineage.Internal.Models
{
    internal sealed class StreamLineage
    {
        private readonly Dictionary<string, StreamLineageInput> _inputsByKey;

        public StreamLineage(
            string builderStreamName,
            string? substreamName,
            DateTimeOffset buildTime,
            IReadOnlyList<StreamLineageInput> inputs,
            IReadOnlyList<StreamLineageOutput> outputs,
            IReadOnlyList<StreamLineageCheck>? checks = null)
        {
            BuilderStreamName = builderStreamName;
            SubstreamName = substreamName;
            BuildTime = TruncateToMicroseconds(buildTime);
            Inputs = inputs;
            Outputs = outputs;
            Checks = checks ?? [];

            // Built eagerly, readers share it across threads.
            _inputsByKey = new Dictionary<string, StreamLineageInput>(StringComparer.Ordinal);
            foreach (var input in inputs)
            {
                _inputsByKey.Add(input.Key, input);
            }
        }

        // Builder name, doubles as the OpenLineage job name.
        public string BuilderStreamName { get; }

        public string? SubstreamName { get; }

        public DateTimeOffset BuildTime { get; }

        public IReadOnlyList<StreamLineageInput> Inputs { get; }

        public IReadOnlyList<StreamLineageOutput> Outputs { get; }

        // Known only after the operators are built.
        public IReadOnlyList<StreamLineageCheck> Checks { get; }

        public StreamLineage WithChecks(IReadOnlyList<StreamLineageCheck> checks)
        {
            return new StreamLineage(BuilderStreamName, SubstreamName, BuildTime, Inputs, Outputs, checks);
        }

        public bool TryGetInput(string key, [NotNullWhen(true)] out StreamLineageInput? input)
        {
            return _inputsByKey.TryGetValue(key, out input);
        }

        private static DateTimeOffset TruncateToMicroseconds(DateTimeOffset time)
        {
            var ticks = time.UtcTicks;
            return new DateTimeOffset(ticks - (ticks % 10), TimeSpan.Zero);
        }
    }
}
