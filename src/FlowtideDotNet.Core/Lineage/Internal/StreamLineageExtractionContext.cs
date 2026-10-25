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

using FlowtideDotNet.Substrait;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal sealed class StreamLineageExtractionContext
    {
        public required Plan Plan { get; init; }

        public required IConnectorManager ConnectorManager { get; init; }

        public required string BuilderStreamName { get; init; }

        // Null means every root is built here.
        public string? SubstreamScope { get; init; }

        public bool IncludeConnectorSchema { get; init; }

        public DateTimeOffset BuildTime { get; init; } = DateTimeOffset.UtcNow;
    }
}
