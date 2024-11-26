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
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.ComputeTests
{
    internal class TestCaseVisitor : FuncTestCaseParserBaseVisitor<object>
    {
        public TestCaseVisitor()
        {
        }

        public override object VisitDoc([NotNull] FuncTestCaseParser.DocContext context)
        {
            var headerInfo = HeaderParser.ParseHeader(context.header());

            var testGroups = context.testGroup();

            foreach (var testGroup in testGroups)
            {
                var testGroupVisitor = new TestGroupVisitor();
                testGroupVisitor.Visit(testGroup);
            }

            return base.VisitDoc(context);
        }

        //public override object VisitHeader([NotNull] FuncTestCaseParser.HeaderContext context)
        //{
        //    var versionInfo = VersionParser.ParseVersion(context.version());
        //    var includeInfo = IncludeParser.ParseInclude(context.include());
        //    return base.VisitHeader(context);
        //}

        //public override object VisitVersion([NotNull] FuncTestCaseParser.VersionContext context)
        //{
        //    var scalar = context.SubstraitScalarTest();
        //    var aggregate = context.SubstraitAggregateTest();
        //    return base.VisitVersion(context);
        //}
    }
}
