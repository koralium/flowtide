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

using FlexBuffers;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Type;
using System.Globalization;
using Xunit;
using LinqExpression = System.Linq.Expressions.Expression;

namespace FlowtideDotNet.Core.Tests.Compute;

public class ModulusTests
{
    private static FunctionsRegister Builtins()
    {
        var register = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(register);
        return register;
    }

    private static ScalarFunction Function(long? left, long? right) => new()
    {
        ExtensionUrn = FunctionsArithmetic.Urn,
        ExtensionName = FunctionsArithmetic.Modulus,
        Arguments = [Literal(left), Literal(right)],
        Options = new()
        {
            ["division_type"] = "TRUNCATE",
            ["overflow"] = "ERROR",
            ["on_domain_error"] = "NULL"
        }
    };

    private static Expression Literal(long? value) => value.HasValue
        ? new NumericLiteral { Value = value.Value }
        : new NullLiteral();

    private static Func<FlxValue> CompileRow(ScalarFunction function, FunctionsRegister register)
    {
        var visitor = new FlowtideExpressionVisitor(register, typeof(object));
        return LinqExpression.Lambda<Func<FlxValue>>(visitor.Visit(function, new ParametersInfo([], []))!).Compile();
    }

    [Theory]
    [InlineData(7L, 3L, 1L)]
    [InlineData(-7L, 3L, -1L)]
    [InlineData(7L, -3L, 1L)]
    [InlineData(-7L, -3L, -1L)]
    [InlineData(7L, 0L, null)]
    [InlineData(0L, 0L, null)]
    [InlineData(null, 3L, null)]
    [InlineData(7L, null, null)]
    public void Row_and_column_execution_match_the_standard_profile(long? left, long? right, long? expected)
    {
        var register = Builtins();
        var function = Function(left, right);
        var column = ColumnProjectCompiler.CompileToValue(function, register)(new EventBatchData([]), 0);
        var row = CompileRow(function, register)();
        if (expected.HasValue)
        {
            Assert.Equal(ArrowTypeId.Int64, column.Type);
            Assert.Equal(expected.Value, column.AsLong);
            Assert.Equal(FlexBuffers.Type.Int, row.ValueType);
            Assert.Equal(expected.Value, row.AsLong);
        }
        else
        {
            Assert.Equal(ArrowTypeId.Null, column.Type);
            Assert.Equal(FlexBuffers.Type.Null, row.ValueType);
        }
        Assert.Equal(3, function.Options!.Count);
    }

    [Fact]
    public void Integer_zero_divisors_return_null_without_options()
    {
        var register = Builtins();
        var function = Function(7, 0);
        function.Options = null;
        Assert.Equal(ArrowTypeId.Null, ColumnProjectCompiler.CompileToValue(function, register)(new EventBatchData([]), 0).Type);
        Assert.Equal(FlexBuffers.Type.Null, CompileRow(function, register)().ValueType);
    }

    [Fact]
    public void Int64_division_overflow_throws()
    {
        var register = Builtins();
        var function = Function(long.MinValue, -1);
        var column = ColumnProjectCompiler.CompileToValue(function, register);
        var row = CompileRow(function, register);
        Assert.Throws<OverflowException>(() => column(new EventBatchData([]), 0));
        Assert.Throws<OverflowException>(() => row());
    }

    [Theory]
    [InlineData("division_type", "FLOOR")]
    [InlineData("overflow", "SATURATE")]
    [InlineData("on_domain_error", "ERROR")]
    [InlineData("unknown", "NULL")]
    public void Unsupported_standard_options_are_rejected(string name, string value)
    {
        var register = Builtins();
        var function = Function(7, 3);
        function.Options![name] = value;
        Assert.Throws<NotSupportedException>(() => ColumnProjectCompiler.CompileToValue(function, register));
        Assert.Throws<NotSupportedException>(() => CompileRow(function, register));
    }

    private static ScalarFunction DecimalFunction(string? left, string? right) => new()
    {
        ExtensionUrn = FunctionsArithmetic.DecimalUrn,
        ExtensionName = FunctionsArithmetic.Modulus,
        Arguments = [DecimalLiteral(left), DecimalLiteral(right)],
        Options = new() { ["overflow"] = "ERROR" }
    };

    private static Expression DecimalLiteral(string? value) => value == null ? new NullLiteral() : new CastExpression
    {
        Expression = new NumericLiteral { Value = decimal.Parse(value, CultureInfo.InvariantCulture) },
        Type = new DecimalType()
    };

    [Theory]
    [InlineData("7.50", "3.000", "1.50")]
    [InlineData("-7.50", "3.000", "-1.50")]
    [InlineData("7.50", "-3.000", "1.50")]
    [InlineData("1.0", "2.00", "1.0")]
    [InlineData(null, "3.00", null)]
    public void Decimal_row_and_column_execution_match_the_standard_profile(string? left, string? right, string? expected)
    {
        var register = Builtins();
        var function = DecimalFunction(left, right);
        var column = ColumnProjectCompiler.CompileToValue(function, register)(new EventBatchData([]), 0);
        var row = CompileRow(function, register)();
        if (expected == null)
        {
            Assert.Equal(ArrowTypeId.Null, column.Type);
            Assert.Equal(FlexBuffers.Type.Null, row.ValueType);
        }
        else
        {
            var value = decimal.Parse(expected, CultureInfo.InvariantCulture);
            Assert.Equal(ArrowTypeId.Decimal128, column.Type);
            Assert.Equal(value, column.AsDecimal);
            Assert.Equal(FlexBuffers.Type.Decimal, row.ValueType);
            Assert.Equal(value, row.AsDecimal);
        }
    }

    [Fact]
    public void Decimal_zero_divisors_throw()
    {
        var register = Builtins();
        var function = DecimalFunction("7.50", "0.00");
        var column = ColumnProjectCompiler.CompileToValue(function, register);
        var row = CompileRow(function, register);
        Assert.Throws<DivideByZeroException>(() => column(new EventBatchData([]), 0));
        Assert.Throws<DivideByZeroException>(() => row());
    }

    [Theory]
    [InlineData("overflow", "SILENT")]
    [InlineData("on_domain_error", "NULL")]
    [InlineData("division_type", "TRUNCATE")]
    public void Decimal_modulus_rejects_unsupported_options(string name, string value)
    {
        var register = Builtins();
        var function = DecimalFunction("7.50", "3.00");
        function.Options![name] = value;
        Assert.Throws<NotSupportedException>(() => ColumnProjectCompiler.CompileToValue(function, register));
        Assert.Throws<NotSupportedException>(() => CompileRow(function, register));
    }
}
