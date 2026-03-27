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
using FlowtideDotNet.Base;
using FlowtideDotNet.Core.Operators.Write;
using Grpc.Core;

namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Provides configuration options for the SpiceDB relationship sink connector.
    /// </summary>
    /// <remarks>
    /// Pass an instance of this class to
    /// <see cref="SpiceDbConnectorManagerExtensions.AddSpiceDbSink"/> to configure the gRPC
    /// connection, write behaviour, and lifecycle callbacks for the SpiceDB sink.
    /// </remarks>
    public class SpiceDbSinkOptions
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
        /// Gets or sets the maximum number of relationship mutations to include in a single
        /// <c>WriteRelationships</c> request. Defaults to <c>50</c>.
        /// </summary>
        public int BatchSize { get; set; } = 50;

        /// <summary>
        /// Gets or sets an optional filter that scopes which existing SpiceDB relationships are
        /// candidates for deletion after the initial data load has completed.
        /// </summary>
        /// <remarks>
        /// When set, the sink reads all relationships from SpiceDB that match this filter after
        /// the initial data load. A relationship is deleted only if it satisfies both conditions:
        /// it matches this filter <em>and</em> it is not present in the result set.
        /// Relationships that do not match the filter are never deleted, regardless of whether
        /// they appear in the result set.
        /// </remarks>
        public ISpiceDbReadRelationshipsRequest? DeleteExistingDataFilter { get; set; }

        /// <summary>
        /// Gets or sets an optional asynchronous callback invoked before each
        /// <see cref="ISpiceDbWriteRelationshipRequest"/> is sent to SpiceDB.
        /// </summary>
        /// <remarks>
        /// Use this callback to inspect or modify the write request before it is submitted,
        /// for example to add preconditions or adjust relationship updates.
        /// </remarks>
        public Func<ISpiceDbWriteRelationshipRequest, Task>? BeforeWriteRequestFunc { get; set; }

        /// <summary>
        /// Gets or sets an optional asynchronous callback invoked each time a new watermark
        /// is received after a successful batch write.
        /// </summary>
        /// <remarks>
        /// The first argument is the <see cref="Watermark"/> from the stream. The second
        /// argument is the <see cref="ISpiceDbZedToken.Token"/> string returned by SpiceDB for
        /// the last committed write. Use this to track which source data has been successfully
        /// written to SpiceDB.
        /// </remarks>
        public Func<Watermark, string, Task>? OnWatermarkFunc { get; set; }

        /// <summary>
        /// Gets or sets an optional asynchronous callback invoked once the initial data load
        /// has been fully committed to SpiceDB.
        /// </summary>
        /// <remarks>
        /// Use this callback to perform post-load cleanup. For example, if an external store
        /// tracks tuples managed by this integration, it can delete any stale tuples that were
        /// not present in the initial result set.
        /// </remarks>
        public Func<Task>? OnInitialDataSentFunc { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of <c>WriteRelationships</c> requests that may be
        /// in flight concurrently. Defaults to <c>4</c>.
        /// </summary>
        public int MaxParallellCalls { get; set; } = 4;

        /// <summary>
        /// Gets or sets the execution mode for the sink operator. Defaults to
        /// <see cref="ExecutionMode.Hybrid"/>.
        /// </summary>
        public ExecutionMode ExecutionMode { get; set; } = ExecutionMode.Hybrid;
    }
}
