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

using Antlr4.Runtime.Misc;
using FlowtideDotNet.ComputeTests.Internal.Tests;

namespace FlowtideDotNet.ComputeTests.Internal.Parser
{
    internal class AggregateTestGroupVisitor : FuncTestCaseParserBaseVisitor<TestGroup>
    {
        public override TestGroup VisitAggregateFuncTestGroup([NotNull] FuncTestCaseParser.AggregateFuncTestGroupContext context)
        {
            var cases = context.aggFuncTestCase();

            List<TestCase> testCases = new List<TestCase>();

            foreach (var testCase in cases)
            {
                testCases.Add(new TestCase()
                {
                    Text = testCase.GetText(),
                    LineNumber = testCase.Start.Line
                });
            }
            var description = TestGroupDescriptionParser.ParseTestGroupDescription(context.testGroupDescription());
            return new TestGroup(description, testCases);
        }
    }
}
