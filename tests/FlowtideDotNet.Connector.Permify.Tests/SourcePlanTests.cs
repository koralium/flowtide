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

using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Grpc.Net.Client;

namespace FlowtideDotNet.Connector.Permify.Tests
{
    public class SourcePlanTests
    {
        // The filter reads the first column, which is not selected
        private static ReadRelation Read(bool filter = true, bool emit = true)
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = ["table"] },
                BaseSchema = new NamedStruct()
                {
                    Names = ["entity_type", "subject_id", "entity_id"],
                    Struct = new Struct() { Types = [new AnyType(), new AnyType(), new AnyType()] }
                },
                Filter = filter ? new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = 0 } } : null,
                Emit = emit ? [2, 1] : null
            };
        }

        // The source sends one column per base schema name
        private static void AssertReadDeclaresWhatIsSent(ReadRelation read)
        {
            Assert.False(read.EmitSet);
            Assert.Equal(read.BaseSchema.Names.Count, read.OutputLength);
            Assert.Equal(read.BaseSchema.Names.Count, read.BaseSchema.Struct!.Types.Count);
        }

        private static Relation ModifyPlan(ReadRelation read)
        {
            var manager = new ConnectorManager();
            manager.AddPermifyRelationshipSource("*", new PermifySourceOptions()
            {
                Channel = GrpcChannel.ForAddress("http://localhost:1"),
                TenantId = "t1"
            });
            return manager.GetSourceFactory(read).ModifyPlan(read);
        }

        [Fact]
        public void TheWrapperFiltersAndEmits()
        {
            var read = Read();
            var filter = read.Filter;

            var normalization = Assert.IsType<NormalizationRelation>(ModifyPlan(read));

            Assert.Same(read, normalization.Input);
            Assert.Equal(filter, normalization.Filter);
            Assert.Equal([2, 1], normalization.Emit);
            Assert.Equal(2, normalization.OutputLength);
            Assert.Equal(6, normalization.KeyIndex.Count);
            AssertReadDeclaresWhatIsSent(read);
            Assert.Null(read.Filter);
        }

        [Fact]
        public void AReadWithoutEmitAlsoEmitsTheKeyColumns()
        {
            var read = Read(emit: false);

            var normalization = Assert.IsType<NormalizationRelation>(ModifyPlan(read));

            Assert.False(normalization.EmitSet);
            Assert.Equal(read.BaseSchema.Names.Count, normalization.OutputLength);
            AssertReadDeclaresWhatIsSent(read);
        }
    }
}
