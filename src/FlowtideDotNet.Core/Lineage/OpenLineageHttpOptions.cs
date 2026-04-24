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

namespace FlowtideDotNet.Core.Lineage
{
    /// <summary>
    /// Configuration options for reporting OpenLineage events over HTTP.
    /// </summary>
    /// <remarks>
    /// These options are used by the OpenLineage HTTP reporter to send lineage events
    /// to an OpenLineage-compatible endpoint. Use <see cref="Url"/> to specify the target endpoint
    /// and <see cref="OnRequest"/> to customize outgoing HTTP requests (for example, to add authentication headers).
    /// The options can be configured through <c>FlowtideBuilder.WithOpenLineageHttp</c> or
    /// the <c>IFlowtideDIBuilder.AddOpenLineageHttp</c> extension method.
    /// </remarks>
    public class OpenLineageHttpOptions
    {
        /// <summary>
        /// Gets or sets the URL of the OpenLineage HTTP endpoint to send lineage events to.
        /// </summary>
        /// <remarks>
        /// This property is required and must be set before the reporter is started.
        /// An <see cref="ArgumentException"/> is thrown at initialization if this value is <see langword="null"/>.
        /// </remarks>
        public string? Url { get; set; }

        /// <summary>
        /// Gets or sets an optional callback that is invoked on each outgoing <see cref="HttpRequestMessage"/>
        /// before it is sent to the OpenLineage endpoint.
        /// </summary>
        /// <remarks>
        /// Use this to modify the HTTP request, for example to add custom headers such as
        /// authorization tokens or API keys.
        /// </remarks>
        public Action<HttpRequestMessage>? OnRequest { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether schema information should be included in the lineage events.
        /// </summary>
        public bool IncludeSchema { get; set; }

        /// <summary>
        /// Gets or sets an optional run identifier for the lineage events.
        /// </summary>
        /// <remarks>
        /// When set, this <see cref="Guid"/> is used as the run ID for all lineage events emitted by the reporter.
        /// If <see langword="null"/>, a new <see cref="Guid"/> is generated automatically.
        /// </remarks>
        public Guid? RunId { get; set; }
    }
}
