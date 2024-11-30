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
using FlowtideDotNet.ComputeTests.SourceGenerator.Internal.Tests;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.ComputeTests.Internal.Tests
{
    internal class ArgumentVisitor : FuncTestCaseParserBaseVisitor<string>
    {
        public override string VisitArgument([NotNull] FuncTestCaseParser.ArgumentContext context)
        {
            return base.VisitArgument(context);
        }

        public override string VisitBooleanArg([NotNull] FuncTestCaseParser.BooleanArgContext context)
        {
            var boolLiteral = context.BooleanLiteral().GetText();

            if (boolLiteral == "true")
            {
                return "BoolValue.True";
            }
            else
            {
                return "BoolValue.False";
            }
        }

        public override string VisitIntegerLiteral([NotNull] FuncTestCaseParser.IntegerLiteralContext context)
        {
            var number = int.Parse(context.GetText());
            return $"new Int64Value({number})";
        }

        public override string VisitFixedBinaryArg([NotNull] FuncTestCaseParser.FixedBinaryArgContext context)
        {
            throw new NotImplementedException();
        }

        public override string VisitStringArg([NotNull] FuncTestCaseParser.StringArgContext context)
        {
            var text = context.StringLiteral().GetText();

            text = text.Trim('\'');

            return $"new StringValue(\"{text}\")";
        }

        public override string VisitFloatArg([NotNull] FuncTestCaseParser.FloatArgContext context)
        {
            var literal = context.numericLiteral();
            var txt = literal.GetText();

            return DataTypeValueWriter.WriteDataValue(txt, "fp64");
            //if (txt == "inf")
            //{
            //    return "new DoubleValue(double.PositiveInfinity)";
            //}
            //else if (txt == "-inf")
            //{
            //    return "new DoubleValue(double.NegativeInfinity)";
            //}

            //var val = double.Parse(literal.GetText(), CultureInfo.InvariantCulture);

            //return $"new DoubleValue({val.ToString(CultureInfo.InvariantCulture)})";
        }

        public override string VisitIntArg([NotNull] FuncTestCaseParser.IntArgContext context)
        {
            var literal = context.IntegerLiteral();
            var val = long.Parse(literal.GetText());
            return $"new Int64Value({val})";
        }

        public override string VisitNullArg([NotNull] FuncTestCaseParser.NullArgContext context)
        {
            return "NullValue.Instance";
        }
    }
}
