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
    internal class SubstraitTestAssemblyRunnerContext(
    SubstraitTestAssembly testAssembly,
    IReadOnlyCollection<SubstraitTestCase> testCases,
    IMessageSink executionMessageSink,
    ITestFrameworkExecutionOptions executionOptions) :
        TestAssemblyRunnerContext<SubstraitTestAssembly, SubstraitTestCase>(testAssembly, testCases, executionMessageSink, executionOptions, default)
    { }
    internal class SubstraitTestAssemblyRunner
        : TestAssemblyRunner<SubstraitTestAssemblyRunnerContext, SubstraitTestAssembly, SubstraitTestCollection, SubstraitTestCase>
    {
        public static SubstraitTestAssemblyRunner Instance { get; } = new();


        protected override ValueTask<string> GetTestFrameworkDisplayName(SubstraitTestAssemblyRunnerContext ctxt)
        {
            return new ValueTask<string>("Substrait Compute Framework");
        }

        public async ValueTask<RunSummary> Run(
        SubstraitTestAssembly testAssembly,
        IReadOnlyCollection<SubstraitTestCase> testCases,
        IMessageSink executionMessageSink,
        ITestFrameworkExecutionOptions executionOptions)
        {
            await using var ctxt = new SubstraitTestAssemblyRunnerContext(testAssembly, testCases, executionMessageSink, executionOptions);
            await ctxt.InitializeAsync();

            return await Run(ctxt);
        }

        protected override async ValueTask<RunSummary> RunTestCollection(SubstraitTestAssemblyRunnerContext ctxt, SubstraitTestCollection testCollection, IReadOnlyCollection<SubstraitTestCase> testCases)
        {
            return await SubstraitTestCollectionRunner.Instance.Run(
                testCollection,
                testCases,
                ctxt.MessageBus,
                ctxt.Aggregator.Clone(),
                ctxt.CancellationTokenSource
            );
        }
    }
}
