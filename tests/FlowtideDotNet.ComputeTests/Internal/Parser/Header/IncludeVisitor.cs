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

namespace FlowtideDotNet.ComputeTests.Internal.Header
{
    internal class IncludeVisitor : FuncTestCaseParserBaseVisitor<IncludeResult>
    {
        public override IncludeResult VisitInclude([NotNull] FuncTestCaseParser.IncludeContext context)
        {
            var stringLiterals = context.StringLiteral();

            List<string> result = new List<string>();
            foreach (var literal in stringLiterals)
            {
                var literalText = literal.GetText();
                literalText = literalText.Trim('\'');
                result.Add(literalText);
            }
            return new IncludeResult(result);
        }


    }
}
