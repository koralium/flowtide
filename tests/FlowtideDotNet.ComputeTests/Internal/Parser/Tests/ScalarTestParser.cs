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

using Antlr4.Runtime;
using FlowtideDotNet.ComputeTests.Internal.Tests;

namespace FlowtideDotNet.ComputeTests.Internal.Parser.Tests
{
    internal static class ScalarTestParser
    {
        public static ScalarTestCase Parse(string txt)
        {
            ICharStream stream = CharStreams.fromString(txt);
            var lexer = new FuncTestCaseLexer(stream);
            ITokenStream tokens = new CommonTokenStream(lexer);
            FuncTestCaseParser parser = new FuncTestCaseParser(tokens)
            {
                BuildParseTree = true
            };
            parser.RemoveErrorListeners();

            var context = parser.testCase();

            var visitor = new ScalarTestVisitor();
            var testCase = visitor.Visit(context);
            return testCase;
        }
    }
}
