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

namespace FlowtideDotNet.ComputeTests
{
    internal class TestCaseParser
    {
        public TestDocument Parse(string text)
        {
            ICharStream stream = CharStreams.fromString(text);
            var lexer = new FuncTestCaseLexer(stream);
            ITokenStream tokens = new CommonTokenStream(lexer);
            FuncTestCaseParser parser = new FuncTestCaseParser(tokens)
            {
                BuildParseTree = true
            };

            var context = parser.doc();

            var visitor = new TestDocumentVisitor();
            var testDoc = visitor.Visit(context);
            return testDoc;
        }
    }
}
