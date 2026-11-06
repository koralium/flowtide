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

using FlowtideDotNet.Connector.StarRocks.Internal;
using FlowtideDotNet.Core.Operators.Write;
using Polly;
using Polly.Retry;

namespace FlowtideDotNet.Connector.StarRocks
{
    public class StarRocksSinkOptions
    {
        public StarRocksSinkOptions()
        {
            ResiliencePipeline = CreateDefaultResiliencePipeline(TimeSpan.FromSeconds(1));
        }

        public required string HttpUrl { get; set; }

        /// <summary>
        /// Optional backend http url mostly used for unit testing with the allin1 docker image.
        /// This allows insertions even when using random ports.
        /// </summary>
        public string? BackendHttpUrl { get; set; }

        public required Func<StarRocksUsernamePassword> Credentials { get; set; }


        public int BatchSize { get; set; } = 1000;

        /// <summary>
        /// Max stream loads in flight within one upload, OnWatermark and Hybrid only.
        /// Each load is its own StarRocks transaction.
        ///
        /// Default: 1
        /// </summary>
        public int MaxParallelLoads { get; set; } = 1;

        /// <summary>
        /// When the connector should commit data to starrocks
        /// On checkpoint allows exactly-once semantics but may have higher latency
        /// 
        /// Default: OnCheckpoint
        /// </summary>
        public ExecutionMode ExecutionMode { get; set; } = ExecutionMode.OnCheckpoint;

        /// <summary>
        /// Resilience pipeline for every request to StarRocks.
        /// Each failed attempt is logged as a warning.
        ///
        /// Default: 5 retries, exponential backoff from 1 second with jitter,
        /// any error except the stream shutting down.
        /// Use <see cref="ResiliencePipeline.Empty"/> to disable retries.
        /// </summary>
        public ResiliencePipeline ResiliencePipeline { get; set; }

        internal IStarRocksClientFactory ClientFactory { get; set; } = new DefaultStarRocksClientFactory();

        internal static ResiliencePipeline CreateDefaultResiliencePipeline(TimeSpan baseDelay)
        {
            return new ResiliencePipelineBuilder()
                .AddRetry(new RetryStrategyOptions
                {
                    MaxRetryAttempts = 5,
                    BackoffType = DelayBackoffType.Exponential,
                    Delay = baseDelay,
                    UseJitter = true,
                    // HttpClient timeouts are cancellations too, only a stopping stream is final
                    ShouldHandle = args => ValueTask.FromResult(
                        args.Outcome.Exception != null &&
                        !(args.Outcome.Exception is OperationCanceledException && args.Context.CancellationToken.IsCancellationRequested))
                })
                .Build();
        }
    }
}
