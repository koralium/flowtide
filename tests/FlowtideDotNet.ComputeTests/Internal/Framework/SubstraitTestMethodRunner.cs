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

using Xunit.v3;

namespace FlowtideDotNet.ComputeTests.Internal.Framework
{
    internal class SubstraitTestMethodRunnerContext(
    SubstraitTestMethod testMethod,
    IReadOnlyCollection<SubstraitTestCase> testCases,
    IMessageBus messageBus,
    ExceptionAggregator aggregator,
    CancellationTokenSource cancellationTokenSource) :
        TestMethodRunnerContext<SubstraitTestMethod, SubstraitTestCase>(testMethod, testCases, Xunit.Sdk.ExplicitOption.Off, messageBus, aggregator, cancellationTokenSource)
    {
    }

    internal class SubstraitTestMethodRunner
        : TestMethodRunner<SubstraitTestMethodRunnerContext, SubstraitTestMethod, SubstraitTestCase>
    {
        public static SubstraitTestMethodRunner Instance { get; } = new();

        public async ValueTask<RunSummary> Run(
            SubstraitTestMethod testMethod,
            IReadOnlyCollection<SubstraitTestCase> testCases,
            IMessageBus messageBus,
            ExceptionAggregator aggregator,
            CancellationTokenSource cancellationTokenSource)
        {
            await using var ctxt = new SubstraitTestMethodRunnerContext(testMethod, testCases, messageBus, aggregator, cancellationTokenSource);
            await ctxt.InitializeAsync();

            return await Run(ctxt);
        }


        protected override ValueTask<RunSummary> RunTestCase(SubstraitTestMethodRunnerContext ctxt, SubstraitTestCase testCase)
        {
            return SubstraitTestCaseRunner.Instance.Run(testCase, ctxt.MessageBus, ctxt.Aggregator.Clone(), ctxt.CancellationTokenSource);
        }
    }
}
