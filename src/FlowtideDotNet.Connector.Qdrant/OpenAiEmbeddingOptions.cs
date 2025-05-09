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

using Polly;
using Polly.RateLimiting;
using System.Threading.RateLimiting;

namespace FlowtideDotNet.Connector.Qdrant
{
    public class OpenAiEmbeddingOptions
    {
        public OpenAiEmbeddingOptions()
        {
            ResiliencePipeline = new ResiliencePipelineBuilder()
                .AddRetry(new Polly.Retry.RetryStrategyOptions
                {
                    MaxRetryAttempts = 5,
                    DelayGenerator = static args =>
                    {
                        if (args.Outcome.Exception is RateLimiterRejectedException rEx)
                        {
                            if (rEx.RetryAfter is not null)
                            {
                                return ValueTask.FromResult(rEx.RetryAfter);
                            }

                            return ValueTask.FromResult<TimeSpan?>(TimeSpan.FromMinutes(1));
                        }

                        var seconds = args.AttemptNumber == 1 ? 1 : args.AttemptNumber * 5;
                        return ValueTask.FromResult<TimeSpan?>(TimeSpan.FromSeconds(seconds));
                    }
                })
                .AddRateLimiter(new SlidingWindowRateLimiter(
                    new SlidingWindowRateLimiterOptions
                    {
                        PermitLimit = MaxRequestsPerMinute,
                        Window = TimeSpan.FromMinutes(1),
                        SegmentsPerWindow = 1,
                        QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
                    }
                    ))
                .Build();
        }

        public required Func<string> UrlFunc { get; init; }

        public required Func<string> ApiKeyFunc { get; init; }

        public int MaxRequestsPerMinute { get; init; } = 900;

        /// <summary>
        /// Resilience pipeline used to communincate with the OpenAI API.
        /// The default pipeline includes a retry strategy and a rate limiter.
        ///<para />
        ///<see cref="MaxRequestsPerMinute"/> is used to configure the rate limiter.
        /// </summary>
        public ResiliencePipeline ResiliencePipeline { get; init; }
    }
}