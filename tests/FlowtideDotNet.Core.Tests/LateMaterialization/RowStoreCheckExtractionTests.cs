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

using FlowtideDotNet.Core.Optimizer.CheckExtraction;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class RowStoreCheckExtractionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ACheckCannotMoveTheEmitOfAStoredHost(bool normalization)
    {
        var read = new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["t"] },
            BaseSchema = new NamedStruct
            {
                Names = ["key", "payload", "flag"],
                Struct = new Struct { Types = [new Int64Type(), new StringType(), new BoolType()] }
            }
        };
        var store = new RowStoreDefinition { Name = "rows", StoredColumns = [1] };
        var condition = new ScalarFunction
        {
            ExtensionUrn = FunctionsCheck.Urn,
            ExtensionName = FunctionsCheck.CheckTrue,
            Arguments =
            [
                new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = 2 } },
                new StringLiteral { Value = "flag must be true" }
            ]
        };
        Relation host;
        if (normalization)
        {
            host = new NormalizationRelation
            {
                Input = read,
                KeyIndex = [0],
                Emit = [0, 2, 1],
                RowStore = store,
                Filter = condition
            };
        }
        else
        {
            read.Emit = [0, 2, 1];
            read.RowStore = store;
            read.Filter = condition;
            host = read;
        }

        var exception = Assert.Throws<InvalidOperationException>(() => CheckExtractor.Extract(new Plan { Relations = [host] }));

        Assert.Contains("row store 'rows' and a check in its filter", exception.Message);
        Assert.Equal([0, 2, 1], host.Emit);
    }
}
