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

using Xunit.Sdk;
using Xunit.v3;

namespace FlowtideDotNet.ComputeTests.Internal.Framework
{
    internal class SubstraitTestClassRunnerContext(
    SubstraitTestClass testClass,
    IReadOnlyCollection<SubstraitTestCase> testCases,
    IMessageBus messageBus,
    ExceptionAggregator aggregator,
    CancellationTokenSource cancellationTokenSource) :
        TestClassRunnerContext<SubstraitTestClass, SubstraitTestCase>(testClass, testCases, ExplicitOption.Off, messageBus, aggregator, cancellationTokenSource)
    {
    }
    internal class SubstraitTestClassRunner
        : TestClassRunner<SubstraitTestClassRunnerContext, SubstraitTestClass, SubstraitTestMethod, SubstraitTestCase>
    {
        public static SubstraitTestClassRunner Instance { get; } = new();

        protected override IReadOnlyCollection<SubstraitTestCase> OrderTestCases(SubstraitTestClassRunnerContext ctxt) =>
        [.. ctxt.TestCases.OrderBy(tc => tc.Order)];

        public async ValueTask<RunSummary> Run(
        SubstraitTestClass testClass,
        IReadOnlyCollection<SubstraitTestCase> testCases,
        IMessageBus messageBus,
        ExceptionAggregator aggregator,
        CancellationTokenSource cancellationTokenSource)
        {
            await using var ctxt = new SubstraitTestClassRunnerContext(testClass, testCases, messageBus, aggregator, cancellationTokenSource);
            await ctxt.InitializeAsync();

            return await Run(ctxt);
        }


        protected override ValueTask<RunSummary> RunTestMethod(SubstraitTestClassRunnerContext ctxt, SubstraitTestMethod? testMethod, IReadOnlyCollection<SubstraitTestCase> testCases, object?[] constructorArguments)
        {
            return SubstraitTestMethodRunner.Instance.Run(testMethod!, testCases, ctxt.MessageBus, ctxt.Aggregator.Clone(), ctxt.CancellationTokenSource);
        }
    }
}
