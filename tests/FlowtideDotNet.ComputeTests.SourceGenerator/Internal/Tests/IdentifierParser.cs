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

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.ComputeTests.Internal.Tests
{
    internal static class IdentifierParser
    {
        public static string ParseIdentifier(FuncTestCaseParser.IdentifierContext context)
        {
            return new IdentifierVisitor().Visit(context);
        }

        private class IdentifierVisitor : FuncTestCaseParserBaseVisitor<string>
        {
            public override string VisitIdentifier([NotNull] FuncTestCaseParser.IdentifierContext context)
            {
                return context.Identifier().GetText();
            }
        }
    }
}
