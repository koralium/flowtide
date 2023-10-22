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

using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using System.Text.Json.Serialization;

namespace FlowtideDotNet.Base.Metrics
{
    public class StreamGraph
    {
        public StreamGraph(IReadOnlyDictionary<string, GraphNode> nodes, IReadOnlyList<GraphEdge> edges, StreamStateValue state)
        {
            Nodes = nodes;
            Edges = edges;
            State = state;
        }

        [JsonPropertyName("nodes")]
        public IReadOnlyDictionary<string, GraphNode> Nodes { get; }

        [JsonPropertyName("edges")]
        public IReadOnlyList<GraphEdge> Edges { get; }

        [JsonPropertyName("state")]
        public StreamStateValue State { get; }
    }
}
