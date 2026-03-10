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

using Authzed.Api.V1;
using Grpc.Core;

namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Provides configuration options for the SpiceDB relationship source connector.
    /// </summary>
    /// <remarks>
    /// Pass an instance of this class to
    /// <see cref="SpiceDbConnectorManagerExtensions.AddSpiceDbSource"/> to configure the gRPC
    /// connection, request metadata, and read consistency for the SpiceDB source. The source
    /// performs an initial load of relationships via the <c>ReadRelationships</c> RPC and then
    /// streams ongoing changes using the SpiceDB <c>Watch</c> API.
    /// </remarks>
    public class SpiceDbSourceOptions
    {
        /// <summary>
        /// Gets or sets the gRPC channel used to connect to the SpiceDB API.
        /// </summary>
        public required ChannelBase Channel { get; set; }

        /// <summary>
        /// Gets or sets an optional factory function that returns gRPC <see cref="Metadata"/>
        /// to attach to each request, such as an <c>Authorization</c> bearer token header.
        /// </summary>
        public Func<Metadata>? GetMetadata { get; set; }

        /// <summary>
        /// Gets or sets the optional consistency requirement applied to the initial
        /// <c>ReadRelationships</c> requests during the data load phase.
        /// </summary>
        /// <remarks>
        /// When not set, the SpiceDB server applies its default consistency behaviour.
        /// This setting is not applied to the <c>Watch</c> stream used for ongoing change
        /// detection after the initial load. Use <see cref="ISpiceDbConsistency"/> factory
        /// methods to specify an explicit requirement.
        /// </remarks>
        public ISpiceDbConsistency? Consistency { get; set; }
    }
}
