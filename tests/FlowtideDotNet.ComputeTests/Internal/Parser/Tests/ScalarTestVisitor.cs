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
using FlowtideDotNet.ComputeTests.Internal.Parser;
using FlowtideDotNet.ComputeTests.SourceGenerator.Internal.Tests;

namespace FlowtideDotNet.ComputeTests.Internal.Tests
{
    internal class ScalarTestVisitor : FuncTestCaseParserBaseVisitor<ScalarTestCase>
    {
        public override ScalarTestCase VisitTestCase([NotNull] FuncTestCaseParser.TestCaseContext context)
        {
            var functionName = IdentifierParser.ParseIdentifier(context.functionName);
            var arguments = ArgumentParser.ParseArguments(context.arguments());
            var result = ResultParser.ParseExpectedResult(context.result());
            var options = OptionsParser.GetOptions(context.funcOptions());
            return new ScalarTestCase(functionName, arguments, result, options);
        }
    }
}
