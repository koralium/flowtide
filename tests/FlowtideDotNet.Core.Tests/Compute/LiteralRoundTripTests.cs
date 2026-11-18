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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Globalization;

namespace FlowtideDotNet.Core.Tests.Compute
{
    public class LiteralRoundTripTests
    {
        [Theory]
        [InlineData("0.9999999999999998", "0.9999999999999999", true)]
        [InlineData("0.5", "0.5000000000000001", true)]
        [InlineData("2", "2.0000000000000001", true)]
        [InlineData("1.5", "2.5", true)]
        public void ComparisonGivesTheSameResultAfterTheTrip(string left, string right, bool expected)
        {
            var comparison = new ScalarFunction()
            {
                ExtensionUrn = FunctionsComparison.Urn,
                ExtensionName = FunctionsComparison.LessThan,
                Arguments =
                [
                    new NumericLiteral() { Value = decimal.Parse(left, CultureInfo.InvariantCulture) },
                    new NumericLiteral() { Value = decimal.Parse(right, CultureInfo.InvariantCulture) }
                ]
            };
            var project = new ProjectRelation()
            {
                Input = new ReadRelation()
                {
                    NamedTable = new NamedTable() { Names = ["input"] },
                    BaseSchema = new NamedStruct() { Names = ["value"] }
                },
                Expressions = [comparison]
            };
            var plan = new Plan() { Relations = [project] };
            var back = SubstraitDeserializer.DeserializeFromJson(SubstraitSerializer.SerializeToJson(plan));
            var decoded = Assert.IsType<ProjectRelation>(back.Relations[0]).Expressions[0];
            var register = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(register);

            var before = ColumnProjectCompiler.CompileToValue(comparison, register)(new EventBatchData([]), 0).AsBool;
            var after = ColumnProjectCompiler.CompileToValue(decoded, register)(new EventBatchData([]), 0).AsBool;

            Assert.Equal(expected, before);
            Assert.Equal(before, after);
        }
    }
}
