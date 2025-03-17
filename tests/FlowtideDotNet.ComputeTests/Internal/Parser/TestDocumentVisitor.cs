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
using FlowtideDotNet.ComputeTests.Internal.Header;
using FlowtideDotNet.ComputeTests.Internal.Tests;

namespace FlowtideDotNet.ComputeTests.Internal.Parser
{
    internal class TestDocumentVisitor : FuncTestCaseParserBaseVisitor<TestDocument>
    {
        public override TestDocument VisitDoc([NotNull] FuncTestCaseParser.DocContext context)
        {
            var headerInfo = HeaderParser.ParseHeader(context.header());

            List<TestGroup> testGroupList = new List<TestGroup>();
            var testGroups = context.testGroup();
            if (headerInfo.Version.IsScalar)
            {
                foreach (var testGroup in testGroups)
                {
                    var testGroupVisitor = new ScalarTestGroupVisitor();
                    testGroupList.Add(testGroupVisitor.Visit(testGroup));
                }
            }
            else
            {
                foreach (var testGroup in testGroups)
                {
                    var testGroupVisitor = new AggregateTestGroupVisitor();
                    testGroupList.Add(testGroupVisitor.Visit(testGroup));
                }
            }
            

            return new TestDocument()
            {
                Include = headerInfo.Include.IncludePaths,
                IsScalar = headerInfo.Version.IsScalar,
                TestGroups = testGroupList
            };
        }
    }
}
