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

using FlowtideDotNet.Connector.Qdrant.Internal;
using FlowtideDotNet.Core.Operators.Write;
using Polly;
using Polly.Retry;
using Qdrant.Client;
using Qdrant.Client.Grpc;

namespace FlowtideDotNet.Connector.Qdrant
{

    public class QdrantSinkOptions
    {
        public QdrantSinkOptions()
        {
            ResiliencePipeline = new ResiliencePipelineBuilder()
                 .AddRetry(new RetryStrategyOptions
                 {
                     MaxRetryAttempts = 10,
                     DelayGenerator = (args) =>
                     {
                         var seconds = args.AttemptNumber;
                         return ValueTask.FromResult<TimeSpan?>(TimeSpan.FromSeconds(seconds));
                     }
                 })
                 .Build();
        }

        /// <summary>
        /// </summary>
        public required Func<QdrantChannel> QdrantChannelFunc { get; set; }

        /// <summary>
        /// The name of the collection to use.
        /// The collection must be created and existing in Qdrant, flowtide will not create the collection.
        /// </summary>
        public required string CollectionName { get; set; }

        /// <summary>
        /// The column that should be used as the identity.
        /// This column must exist.
        /// </summary>
        public string IdColumnName { get; init; } = "id";

        /// <summary>
        /// The column that contain the data that should be vectorized.
        /// This column must exist.
        /// </summary>
        public string VectorStringColumnName { get; init; } = "vector_string";

        /// <summary>
        /// Extra functionality that should run on intitialization of the flowtide stream. 
        /// For instance if a new collection should be created based on the stream version etc.
        /// </summary>
        public Func<QdrantSinkState, QdrantClient, Task>? OnInitialize { get; init; }

        /// <summary>
        /// Extra functionality that should run when the initial data has been sent to Qdrant.
        /// </summary>
        public Func<QdrantSinkState, QdrantClient, Task>? OnInitialDataSent { get; init; }

        /// <summary>
        /// Extra functionality that should run when changes have been sent to Qdrant.
        /// </summary>
        public Func<QdrantSinkState, QdrantClient, Task>? OnChangesDone { get; init; }

        /// <summary>
        /// Wait on operations to qdrant, if set to false the operation will be done in the background and the result will not be awaited.
        /// </summary>
        public bool Wait { get; init; } = true;

        public ExecutionMode ExecutionMode { get; init; } = ExecutionMode.Hybrid;

        /// <summary>
        /// The property name that should be used for the vector text in the payload.
        /// If chunking is used this will be the chunked text, otherwise it will be the full text.
        /// </summary>
        public string QdrantVectorTextPropertyName { get; init; } = "text";

        /// <summary>
        /// The name of the object which all properties will be added to, this is added to the point payload.
        /// </summary>
        public string QdrantPayloadDataPropertyName { get; init; } = "data";

        /// <summary>
        /// If set to true the <see cref="VectorStringColumnName"/> will be added to the payload with the <see cref="QdrantVectorTextPropertyName"/>.
        /// If set to false it will not be included in the payload.
        /// </summary>
        public bool QdrantIncludeVectorTextInPayload { get; set; } = true;

        /// <summary>
        /// Selected map properties will be added directly under payload and not under <see cref="QdrantPayloadDataPropertyName"/> when this is enabled
        /// </summary>
        public bool QdrantStoreMapsUnderOwnKey { get; init; }

        /// <summary>
        /// Selected list properties will added directly under payload and not under <see cref="QdrantPayloadDataPropertyName"/> when this is enabled
        /// </summary>
        public bool QdrantStoreListsUnderOwnKey { get; init; }

        /// <summary>
        /// The mode to use when updating the payload in Qdrant.
        /// </summary>
        public QdrantPayloadUpdateMode QdrantPayloadUpdateMode { get; set; } = QdrantPayloadUpdateMode.OverwritePayload;

        /// <summary>
        /// Resilience pipeline for operations against Qdrant, default retries 10 times incrementally
        /// </summary>
        public ResiliencePipeline ResiliencePipeline { get; set; }

        /// <summary>
        /// The maximum number of operations to send in a batch to Qdrant.
        /// </summary>
        public int MaxNumberOfBatchOperations { get; set; } = 1000;
    }
}