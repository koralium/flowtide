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
    internal class SubstraitTestCaseRunnerContext(
    SubstraitTestCase testCase,
    IMessageBus messageBus,
    ExceptionAggregator aggregator,
    CancellationTokenSource cancellationTokenSource) :
        TestCaseRunnerContext<SubstraitTestCase, SubstraitTest>(testCase, Xunit.Sdk.ExplicitOption.Off, messageBus, aggregator, cancellationTokenSource)
    {

        public override IReadOnlyCollection<SubstraitTest> Tests =>
            [new SubstraitTest(TestCase)];

    }
    internal class SubstraitTestCaseRunner
        : TestCaseRunner<SubstraitTestCaseRunnerContext, SubstraitTestCase, SubstraitTest>
    {
        public static SubstraitTestCaseRunner Instance { get; } = new();

        public async ValueTask<RunSummary> Run(
        SubstraitTestCase testCase,
        IMessageBus messageBus,
        ExceptionAggregator aggregator,
        CancellationTokenSource cancellationTokenSource)
        {
            await using var ctxt = new SubstraitTestCaseRunnerContext(testCase, messageBus, aggregator, cancellationTokenSource);
            await ctxt.InitializeAsync();

            return await Run(ctxt);
        }

        protected override ValueTask<RunSummary> RunTest(SubstraitTestCaseRunnerContext ctxt, SubstraitTest test)
        {
            return SubstraitTestRunner.Instance.Run(test, ctxt.MessageBus, null, ctxt.Aggregator.Clone(), ctxt.CancellationTokenSource);
        }
    }
}
