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

using Antlr4.Runtime.Misc;
using FlowtideDotNet.ComputeTests.Internal.Tests;
using System;
using System.Collections.Generic;
using System.Text;

namespace FlowtideDotNet.ComputeTests.SourceGenerator.Internal.Tests
{

    internal class AggregateTestGroupVisitor : FuncTestCaseParserBaseVisitor<AggregateTestGroup>
    {

        public override AggregateTestGroup VisitAggregateFuncTestGroup([NotNull] FuncTestCaseParser.AggregateFuncTestGroupContext context)
        {
            var cases = context.aggFuncTestCase();

            List<AggregateTestCase> testCases = new List<AggregateTestCase>();

            foreach (var testCase in cases)
            {
                var testCaseVisitor = new AggregateTestCaseVisitor();
                testCases.Add(testCaseVisitor.Visit(testCase));
            }
            var description = TestGroupDescriptionParser.ParseTestGroupDescription(context.testGroupDescription());
            return new AggregateTestGroup(description, testCases);
        }
    }

    internal record FuncCallResult(string functionName, IReadOnlyList<IReadOnlyList<string>> columns);

    internal class AggregateFuncCallVisitor : FuncTestCaseParserBaseVisitor<FuncCallResult>
    {
        public override FuncCallResult VisitSingleArgAggregateFuncCall([NotNull] FuncTestCaseParser.SingleArgAggregateFuncCallContext context)
        {
            var functionName = IdentifierParser.ParseIdentifier(context.functName);
            var columns = DataColumnParser.ParseDataColumn(context.dataColumn());
            return new FuncCallResult(functionName, columns);
        }
    }

    internal class AggregateTestCaseVisitor : FuncTestCaseParserBaseVisitor<AggregateTestCase>
    {
        public override AggregateTestCase VisitAggFuncTestCase([NotNull] FuncTestCaseParser.AggFuncTestCaseContext context)
        {
            var funcCallResult = new AggregateFuncCallVisitor().Visit(context.aggFuncCall());
            var expectedResult = ResultParser.ParseExpectedResult(context.result());
            var options = OptionsParser.GetOptions(context.funcOptions());
            return new AggregateTestCase(funcCallResult.functionName, funcCallResult.columns, expectedResult, options);
        }
    }
}
