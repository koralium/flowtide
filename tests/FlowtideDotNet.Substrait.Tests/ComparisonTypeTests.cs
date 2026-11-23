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

using FlowtideDotNet.Substrait.Exceptions;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Substrait.Tests
{
    /// <summary>
    /// Numeric literals take the type they are compared with, other type mismatches throw.
    /// </summary>
    public class ComparisonTypeTests
    {
        private static Expression GetFilterCondition(string where)
        {
            var plan = GetPlan($"INSERT INTO output SELECT i FROM t WHERE {where}");
            Relation? relation = plan.Relations[0];
            while (relation != null)
            {
                switch (relation)
                {
                    case FilterRelation filter:
                        return filter.Condition;
                    case WriteRelation write:
                        relation = write.Input;
                        break;
                    case ProjectRelation project:
                        relation = project.Input;
                        break;
                    default:
                        relation = null;
                        break;
                }
            }
            throw new InvalidOperationException("The plan has no filter relation.");
        }

        private static Plan GetPlan(string sql)
        {
            var builder = new SqlPlanBuilder();
            builder.AddTableDefinition("t", new NamedStruct()
            {
                Names = new List<string>() { "i", "f", "d", "s", "ts", "a" },
                Struct = new Struct()
                {
                    Types = new List<SubstraitBaseType>()
                    {
                        new Int64Type(),
                        new Fp64Type(),
                        new DecimalType(),
                        new StringType(),
                        new TimestampType(),
                        AnyType.Instance
                    }
                }
            });
            builder.Sql(sql);
            return builder.GetPlan();
        }

        private static SubstraitType GetOutputType(string select)
        {
            var plan = GetPlan($"INSERT INTO output SELECT {select} AS x FROM t");
            var write = Assert.IsType<WriteRelation>(plan.Relations[0]);
            Assert.NotNull(write.TableSchema.Struct);
            return write.TableSchema.Struct.Types[0].Type;
        }

        [Theory]
        [InlineData("a + 1")]
        [InlineData("1 - a")]
        [InlineData("a * f")]
        [InlineData("d / a")]
        [InlineData("a % 2")]
        public void ArithmeticWithAnyIsAny(string select)
        {
            Assert.Equal(SubstraitType.Any, GetOutputType(select));
        }

        [Theory]
        [InlineData("i / i", SubstraitType.Fp64)]
        [InlineData("i / 2", SubstraitType.Fp64)]
        [InlineData("f / i", SubstraitType.Fp64)]
        [InlineData("d / i", SubstraitType.Decimal)]
        [InlineData("i + 1", SubstraitType.Int64)]
        [InlineData("gettimestamp()", SubstraitType.TimestampTz)]
        public void ExpressionTypes(string select, SubstraitType expected)
        {
            Assert.Equal(expected, GetOutputType(select));
        }

        private static void AssertCastLiteral(Expression expression, SubstraitType type, decimal value)
        {
            var cast = Assert.IsType<CastExpression>(expression);
            Assert.Equal(type, cast.Type.Type);
            var literal = Assert.IsType<NumericLiteral>(cast.Expression);
            Assert.Equal(value, literal.Value);
        }

        [Theory]
        [InlineData("d = 0")]
        [InlineData("d <> 0")]
        [InlineData("d > 0")]
        [InlineData("d >= 0")]
        [InlineData("d < 0")]
        [InlineData("d <= 0")]
        public void DecimalComparedWithIntegerLiteralCastsLiteral(string where)
        {
            var condition = Assert.IsType<ScalarFunction>(GetFilterCondition(where));
            Assert.IsType<DirectFieldReference>(condition.Arguments[0]);
            AssertCastLiteral(condition.Arguments[1], SubstraitType.Decimal, 0);
        }

        [Fact]
        public void LiteralOnLeftSideIsCast()
        {
            var condition = Assert.IsType<ScalarFunction>(GetFilterCondition("0 < d"));
            AssertCastLiteral(condition.Arguments[0], SubstraitType.Decimal, 0);
            Assert.IsType<DirectFieldReference>(condition.Arguments[1]);
        }

        [Fact]
        public void DecimalComparedWithFractionLiteralCastsLiteral()
        {
            var condition = Assert.IsType<ScalarFunction>(GetFilterCondition("d > 1.5"));
            AssertCastLiteral(condition.Arguments[1], SubstraitType.Decimal, 1.5m);
        }

        [Fact]
        public void DoubleComparedWithIntegerLiteralCastsLiteral()
        {
            var condition = Assert.IsType<ScalarFunction>(GetFilterCondition("f <> 0"));
            AssertCastLiteral(condition.Arguments[1], SubstraitType.Fp64, 0);
        }

        [Fact]
        public void NegativeLiteralIsCast()
        {
            var condition = Assert.IsType<ScalarFunction>(GetFilterCondition("d > -5"));
            AssertCastLiteral(condition.Arguments[1], SubstraitType.Decimal, -5);
        }

        [Fact]
        public void IntegerComparedWithIntegerLiteralIsNotCast()
        {
            var condition = Assert.IsType<ScalarFunction>(GetFilterCondition("i > 1"));
            Assert.IsType<NumericLiteral>(condition.Arguments[1]);
        }

        [Fact]
        public void IntegerComparedWithFractionLiteralKeepsLiteral()
        {
            // Casting to an integer would truncate the fraction
            var condition = Assert.IsType<ScalarFunction>(GetFilterCondition("i > 1.5"));
            var literal = Assert.IsType<NumericLiteral>(condition.Arguments[1]);
            Assert.Equal(1.5m, literal.Value);
        }

        [Fact]
        public void BetweenCastsBothBounds()
        {
            var condition = Assert.IsType<ScalarFunction>(GetFilterCondition("d BETWEEN 0 AND 10"));
            var low = Assert.IsType<ScalarFunction>(condition.Arguments[0]);
            var high = Assert.IsType<ScalarFunction>(condition.Arguments[1]);
            AssertCastLiteral(low.Arguments[1], SubstraitType.Decimal, 0);
            AssertCastLiteral(high.Arguments[1], SubstraitType.Decimal, 10);
        }

        [Fact]
        public void EqualityBetweenColumnsOfDifferentTypesThrows()
        {
            var ex = Assert.Throws<SubstraitParseException>(() => GetFilterCondition("d = i"));
            Assert.Equal("Missmatch type in equality: 'd = i', type(Decimal) = type(Int64)", ex.Message);
        }

        [Fact]
        public void ComparisonBetweenColumnsOfDifferentTypesThrows()
        {
            var ex = Assert.Throws<SubstraitParseException>(() => GetFilterCondition("d > i"));
            Assert.Equal("Missmatch type in comparison: 'd > i', type(Decimal) > type(Int64)", ex.Message);
        }

        [Theory]
        [InlineData("f = i")]
        [InlineData("f = d")]
        [InlineData("s = i")]
        [InlineData("s = 1")]
        [InlineData("d <> i")]
        [InlineData("f >= i")]
        [InlineData("f < d")]
        [InlineData("s <= i")]
        [InlineData("s > 1")]
        [InlineData("ts > i")]
        [InlineData("d BETWEEN i AND 10")]
        public void OtherTypeMismatchesThrow(string where)
        {
            Assert.Throws<SubstraitParseException>(() => GetFilterCondition(where));
        }

        [Theory]
        [InlineData("ts < gettimestamp()")]
        [InlineData("i / i > f")]
        [InlineData("a > i")]
        [InlineData("a = s")]
        [InlineData("a + 1 > f")]
        [InlineData("i * a = d")]
        [InlineData("a / 2 < i")]
        public void MatchingOrUnknownTypesDoNotThrow(string where)
        {
            Assert.IsType<ScalarFunction>(GetFilterCondition(where));
        }

        [Fact]
        public void IntegerEqualsFractionLiteralKeepsLiteral()
        {
            var condition = Assert.IsType<ScalarFunction>(GetFilterCondition("i = 1.5"));
            var literal = Assert.IsType<NumericLiteral>(condition.Arguments[1]);
            Assert.Equal(1.5m, literal.Value);
        }
    }
}
