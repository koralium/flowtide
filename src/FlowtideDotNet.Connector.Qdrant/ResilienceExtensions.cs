﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using System.Diagnostics;

namespace FlowtideDotNet.Connector.Qdrant
{
    internal static class ResilienceExtensions
    {
        public static async Task<T> ExecutePipeline<T>(this Task<T> execute, ResiliencePipeline pipeline)
        {
            var context = ResilienceContextPool.Shared.Get();
            var outcome = await pipeline.ExecuteOutcomeAsync(static async (ctx, state) =>
            {
                try
                {
                    var result = await state.Task;
                    return Outcome.FromResult(result);
                }
                catch (Exception ex)
                {
                    return Outcome.FromException<T>(ex);
                }
            }, context, new PipelineState<T>(execute));

            ResilienceContextPool.Shared.Return(context);
            outcome.ThrowIfException();

            Debug.Assert(outcome.Result != null);

            return outcome.Result!;
        }

        private sealed record PipelineState<T>(Task<T> Task);
    }
}
