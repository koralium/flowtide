using Polly;
using System.Diagnostics;
using System.Threading.Tasks;
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

namespace FlowtideDotNet.Storage.SqlServer
{
    internal static class SqlServerStorageExtensionsHelpers
    {
        public static void ExecutePipeline(this Action action, SqlServerPersistentStorageSettings settings)
        {
            var context = ResilienceContextPool.Shared.Get();
            var pipeline = settings.ResiliencePipeline;
            var task = pipeline.ExecuteOutcomeAsync(static (ctx, state) =>
            {
                try
                {
                    state.Action.Invoke();
                    return Outcome.FromResultAsValueTask(new PipelineResult(true));
                }
                catch (Exception ex)
                {
                    return Outcome.FromExceptionAsValueTask<PipelineResult>(ex);
                }
            }, context, new PipelineActionState(action));

            Outcome<PipelineResult> outcome;
            if (task.IsCompletedSuccessfully)
            {
                outcome = task.Result;
            }
            else
            {
                outcome = task.AsTask().GetAwaiter().GetResult();
            }

            Debug.Assert(task.IsCompleted);
            Debug.Assert(task.IsCompletedSuccessfully);

            ResilienceContextPool.Shared.Return(context);
            outcome.ThrowIfException();

            Debug.Assert(outcome.Result != null);
            Debug.Assert(outcome.Result.Success);

        }

        public static T ExecutePipeline<T>(this Func<T> func, SqlServerPersistentStorageSettings settings)
        {
            var context = ResilienceContextPool.Shared.Get();
            var pipeline = settings.ResiliencePipeline;
            var task = pipeline.ExecuteOutcomeAsync(static (ctx, state) =>
            {
                try
                {
                    return Outcome.FromResultAsValueTask(state.Func());
                }
                catch (Exception ex)
                {
                    return Outcome.FromExceptionAsValueTask<T>(ex);
                }
            }, context, new PipelineFuncState<T>(func));

            Outcome<T> outcome;
            if (task.IsCompletedSuccessfully)
            {
                outcome = task.Result;
            }
            else
            {
                outcome = task.AsTask().GetAwaiter().GetResult();
            }

            Debug.Assert(task.IsCompleted);
            Debug.Assert(task.IsCompletedSuccessfully);

            ResilienceContextPool.Shared.Return(context);
            outcome.ThrowIfException();

            Debug.Assert(outcome.Result != null);

            return outcome.Result;
        }

        public static async Task<T> ExecutePipeline<T>(this Task<T> execute, SqlServerPersistentStorageSettings settings)
        {
            var context = ResilienceContextPool.Shared.Get();
            var pipeline = settings.ResiliencePipeline;
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

        public static async Task ExecutePipeline(this Task execute, SqlServerPersistentStorageSettings settings)
        {
            var context = ResilienceContextPool.Shared.Get();
            var pipeline = settings.ResiliencePipeline;

            var outcome = await pipeline.ExecuteOutcomeAsync(static async (ctx, state) =>
            {
                try
                {
                    await state.Task;
                    return Outcome.FromResult(new PipelineResult(true));
                }
                catch (Exception ex)
                {
                    return Outcome.FromException<PipelineResult>(ex);
                }
            }, context, new PipelineState(execute));

            ResilienceContextPool.Shared.Return(context);
            outcome.ThrowIfException();

            Debug.Assert(outcome.Result != null);
            Debug.Assert(outcome.Result.Success);
        }

        private sealed record PipelineState<T>(Task<T> Task);
        private sealed record PipelineState(Task Task);
        private sealed record PipelineActionState(Action Action);
        private sealed record PipelineFuncState<T>(Func<T> Func);
        private sealed record PipelineResult(bool Success);
    }
}