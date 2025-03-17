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

namespace FlowtideDotNet.ComputeTests.SourceGenerator.Internal.Tests
{
    internal class OptionsVisitor : FuncTestCaseParserBaseVisitor<object>
    {
        public override object VisitFuncOptions([NotNull] FuncTestCaseParser.FuncOptionsContext context)
        {
            var options = context.funcOption();
            SortedList<string, string> result = new SortedList<string, string>();
            for(int i = 0; i < options.Length; i++)
            {
                var option = (KeyValuePair<string, string>)VisitFuncOption(options[i]);
                result.Add(option.Key, option.Value);
            }

            return result;
        }

        public override object VisitFuncOption([NotNull] FuncTestCaseParser.FuncOptionContext context)
        {
            var key = Visit(context.optionName()) as string;
            var value = Visit(context.optionValue()) as string;
            return new KeyValuePair<string, string>(key!, value!);
        }

        public override object VisitOptionValue([NotNull] FuncTestCaseParser.OptionValueContext context)
        {
            return context.GetText();

        }
        public override object VisitOptionName([NotNull] FuncTestCaseParser.OptionNameContext context)
        {
            return context.GetText();
        }
    }
}
