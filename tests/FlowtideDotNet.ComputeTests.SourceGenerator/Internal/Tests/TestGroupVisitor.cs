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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.ComputeTests.Internal.Tests
{
    internal class TestGroupVisitor : FuncTestCaseParserBaseVisitor<ScalarTestGroup>
    {
        public override ScalarTestGroup VisitScalarFuncTestGroup([NotNull] FuncTestCaseParser.ScalarFuncTestGroupContext context)
        {
            var description = TestGroupDescriptionParser.ParseTestGroupDescription(context.testGroupDescription());

            var scalarTests = context.testCase();

            List<ScalarTestCase> testCases = new List<ScalarTestCase>();

            var testCaseVisitor = new ScalarTestVisitor();
            foreach (var scalarTest in scalarTests)
            {
                var testCase = testCaseVisitor.VisitTestCase(scalarTest);
                testCases.Add(testCase);
            }
            return new ScalarTestGroup(description, testCases);
        }
    }
}
