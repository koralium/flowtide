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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using System.Globalization;

namespace FlowtideDotNet.ComputeTests.Internal.Tests
{
    internal class ArgumentVisitor : FuncTestCaseParserBaseVisitor<IDataValue>
    {
        public override IDataValue VisitArgument([NotNull] FuncTestCaseParser.ArgumentContext context)
        {
            var listArg = context.listArg();
            var ch = context.children;
            return base.VisitArgument(context);
        }

        public override IDataValue VisitBooleanArg([NotNull] FuncTestCaseParser.BooleanArgContext context)
        {
            var boolLiteral = context.BooleanLiteral().GetText();

            if (boolLiteral == "true")
            {
                return BoolValue.True;
            }
            else
            {
                return BoolValue.False;
            }
        }

        public override IDataValue VisitIntegerLiteral([NotNull] FuncTestCaseParser.IntegerLiteralContext context)
        {
            var number = int.Parse(context.GetText());
            return new Int64Value(number);
        }

        public override IDataValue VisitFixedBinaryArg([NotNull] FuncTestCaseParser.FixedBinaryArgContext context)
        {
            throw new NotImplementedException();
        }

        public override IDataValue VisitStringArg([NotNull] FuncTestCaseParser.StringArgContext context)
        {
            var text = context.StringLiteral().GetText();

            text = text.Trim('\'');

            return new StringValue(text);
        }

        public override IDataValue VisitFloatArg([NotNull] FuncTestCaseParser.FloatArgContext context)
        {
            var literal = context.numericLiteral();
            var txt = literal.GetText();
            if (txt == "inf")
            {
                return new DoubleValue(double.PositiveInfinity);
            }
            else if (txt == "-inf")
            {
                return new DoubleValue(double.NegativeInfinity);
            }
            else if (txt == "nan")
            {
                return new DoubleValue(double.NaN);
            }
            return new DoubleValue(double.Parse(txt, CultureInfo.InvariantCulture));
        }

        public override IDataValue VisitIntArg([NotNull] FuncTestCaseParser.IntArgContext context)
        {
            var literal = context.IntegerLiteral();
            var val = long.Parse(literal.GetText());
            return new Int64Value(val);
        }

        public override IDataValue VisitNullArg([NotNull] FuncTestCaseParser.NullArgContext context)
        {
            return NullValue.Instance;
        }

        public override IDataValue VisitDecimalArg([NotNull] FuncTestCaseParser.DecimalArgContext context)
        {
            var literal = context.numericLiteral();
            var txt = literal.GetText();

            return new DecimalValue(decimal.Parse(txt, CultureInfo.InvariantCulture));
        }

        public override IDataValue VisitTimestampArg([NotNull] FuncTestCaseParser.TimestampArgContext context)
        {
            var literal = context.TimestampLiteral();
            var value = literal.GetText();

            value = value.Trim('\'');
            if (DateTimeOffset.TryParse(value, out var result))
            {
                return new TimestampTzValue(result.Ticks, (short)result.Offset.TotalMinutes);
            }
            throw new Exception($"Could not parse timestamp {value}");
        }

        private string? dataType;

        public override IDataValue VisitListArg([NotNull] FuncTestCaseParser.ListArgContext context)
        {
            var elemType = new ListTypeVisitor().Visit(context.listType());

            dataType = elemType.GetText();
            var literalList = context.literalList();

            List<IDataValue> values = new List<IDataValue>();
            foreach (var literal in literalList.literal())
            {
                values.Add(Visit(literal));
            }
            return new ListValue(values);
        }

        private class ListTypeVisitor : FuncTestCaseParserBaseVisitor<FuncTestCaseParser.DataTypeContext>
        {
            public override FuncTestCaseParser.DataTypeContext VisitList([NotNull] FuncTestCaseParser.ListContext context)
            {
                return context.elemType;
            }
        }

        public override IDataValue VisitLiteral([NotNull] FuncTestCaseParser.LiteralContext context)
        {
            if (dataType == null)
            {
                throw new InvalidOperationException("Data type must be set before visiting a literal");
            }
            return DataTypeValueWriter.WriteDataValue(context.GetText(), dataType);
        }
    }
}
