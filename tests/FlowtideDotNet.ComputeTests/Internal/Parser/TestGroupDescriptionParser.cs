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

namespace FlowtideDotNet.ComputeTests.Internal.Tests
{
    internal static class TestGroupDescriptionParser
    {

        public static string ParseTestGroupDescription(FuncTestCaseParser.TestGroupDescriptionContext context)
        {
            return new Visitor().VisitTestGroupDescription(context);
        }

        private class Visitor : FuncTestCaseParserBaseVisitor<string>
        {
            public override string VisitTestGroupDescription([NotNull] FuncTestCaseParser.TestGroupDescriptionContext context)
            {
                var descriptorLine = context.DescriptionLine();
                var str = descriptorLine.GetText();

                var noHash = str.Substring(2);
                return noHash.TrimEnd('\n').TrimEnd('\r').Trim();
            }
        }
    }
}
