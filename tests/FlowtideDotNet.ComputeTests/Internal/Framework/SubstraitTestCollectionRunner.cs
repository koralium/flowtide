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

using Xunit.Internal;
using Xunit.v3;

namespace FlowtideDotNet.ComputeTests.Internal.Framework
{
    internal class SubstraitTestCollectionRunnerContext(
    SubstraitTestCollection testCollection,
    IReadOnlyCollection<SubstraitTestCase> testCases,
    IMessageBus messageBus,
    ExceptionAggregator aggregator,
    CancellationTokenSource cancellationTokenSource) :
        TestCollectionRunnerContext<SubstraitTestCollection, SubstraitTestCase>(testCollection, testCases, Xunit.Sdk.ExplicitOption.Off, messageBus, aggregator, cancellationTokenSource)
    { }

    internal class SubstraitTestCollectionRunner
        : TestCollectionRunner<SubstraitTestCollectionRunnerContext, SubstraitTestCollection, SubstraitTestClass, SubstraitTestCase>
    {
        public static SubstraitTestCollectionRunner Instance { get; } = new();

        protected override ValueTask<RunSummary> FailTestClass(SubstraitTestCollectionRunnerContext ctxt, SubstraitTestClass? testClass, IReadOnlyCollection<SubstraitTestCase> testCases, Exception exception)
        {
            var result = XunitRunnerHelper.FailTestCases(
                Guard.ArgumentNotNull(ctxt).MessageBus,
                ctxt.CancellationTokenSource,
                testCases,
                exception,
                sendTestClassMessages: true,
                sendTestMethodMessages: true
            );

            return new(result);
        }

        public async ValueTask<RunSummary> Run(
        SubstraitTestCollection testCollection,
        IReadOnlyCollection<SubstraitTestCase> testCases,
        IMessageBus messageBus,
        ExceptionAggregator exceptionAggregator,
        CancellationTokenSource cancellationTokenSource)
        {
            await using var ctxt = new SubstraitTestCollectionRunnerContext(testCollection, testCases, messageBus, exceptionAggregator, cancellationTokenSource);
            await ctxt.InitializeAsync();

            return await Run(ctxt);
        }


        protected override async ValueTask<RunSummary> RunTestClass(SubstraitTestCollectionRunnerContext ctxt, SubstraitTestClass? testClass, IReadOnlyCollection<SubstraitTestCase> testCases)
        {
            var result = await SubstraitTestClassRunner.Instance.Run(testClass!, testCases, ctxt.MessageBus, ctxt.Aggregator.Clone(), ctxt.CancellationTokenSource);
            return result;
        }
    }
}
