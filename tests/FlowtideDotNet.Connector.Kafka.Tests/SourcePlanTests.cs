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
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Connector.Kafka.Tests
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
                    Names = ["only_filtered", "a", "b"],
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

        private static Relation ModifyPlan(ReadRelation read, bool immutable)
        {
            var manager = new ConnectorManager();
            manager.AddKafkaSource("*", new FlowtideKafkaSourceOptions()
            {
                KeyDeserializer = new FlowtidekafkaStringKeyDeserializer(),
                ValueDeserializer = new FlowtideKafkaUpsertJsonDeserializer(),
                IsImmutable = immutable
            });
            return manager.GetSourceFactory(read).ModifyPlan(read);
        }

        [Fact]
        public void TheKeyedWrapperFiltersAndEmits()
        {
            var read = Read();
            var filter = read.Filter;

            var normalization = Assert.IsType<NormalizationRelation>(ModifyPlan(read, false));

            Assert.Same(read, normalization.Input);
            Assert.Equal(filter, normalization.Filter);
            Assert.Equal([2, 1], normalization.Emit);
            Assert.Equal(2, normalization.OutputLength);
            // The key column is added last
            Assert.Equal([3], normalization.KeyIndex);
            Assert.Equal("_key", read.BaseSchema.Names[3]);
            AssertReadDeclaresWhatIsSent(read);
            Assert.Null(read.Filter);
        }

        [Fact]
        public void TheImmutableFilterFiltersAndEmits()
        {
            var read = Read();
            var filter = read.Filter;

            var filterRelation = Assert.IsType<FilterRelation>(ModifyPlan(read, true));

            Assert.Same(read, filterRelation.Input);
            Assert.Equal(filter, filterRelation.Condition);
            Assert.Equal([2, 1], filterRelation.Emit);
            Assert.Equal(2, filterRelation.OutputLength);
            Assert.Equal(3, read.BaseSchema.Names.Count);
            AssertReadDeclaresWhatIsSent(read);
            Assert.Null(read.Filter);
        }

        [Fact]
        public void AnImmutableReadWithOnlyAnEmitGetsAProjection()
        {
            var read = Read(filter: false);

            var projection = Assert.IsType<ProjectRelation>(ModifyPlan(read, true));

            Assert.Same(read, projection.Input);
            Assert.Empty(projection.Expressions);
            Assert.Equal([2, 1], projection.Emit);
            Assert.Equal(2, projection.OutputLength);
            AssertReadDeclaresWhatIsSent(read);
        }

        // Reordered, repeated, shorter and empty emits
        [Theory]
        [InlineData(new int[] { 2, 1, 0 })]
        [InlineData(new int[] { 0, 0, 2 })]
        [InlineData(new int[] { 0, 1 })]
        [InlineData(new int[] { })]
        public void AnImmutableNonidentityEmitGetsAProjection(int[] emit)
        {
            var read = Read(filter: false);
            read.Emit = emit.ToList();

            var projection = Assert.IsType<ProjectRelation>(ModifyPlan(read, true));

            Assert.Same(read, projection.Input);
            Assert.Empty(projection.Expressions);
            Assert.Equal(emit, projection.Emit);
            Assert.Equal(emit.Length, projection.OutputLength);
            AssertReadDeclaresWhatIsSent(read);
        }

        [Fact]
        public void AnImmutableIdentityEmitDoesNotAddAProjection()
        {
            var read = Read(filter: false);
            read.Emit = [0, 1, 2];

            Assert.Same(read, ModifyPlan(read, true));
            AssertReadDeclaresWhatIsSent(read);
        }

        [Fact]
        public void AnOptimizedImmutableIdentityEmitDoesNotAddAProjection()
        {
            var read = Read(filter: false);
            read.Emit = [1, 2];
            var plan = PlanOptimizer.Optimize(new Plan() { Relations = [read] });
            read = Assert.IsType<ReadRelation>(Assert.Single(plan.Relations));

            Assert.Equal(["a", "b"], read.BaseSchema.Names);
            Assert.Equal([0, 1], read.Emit);
            Assert.Same(read, ModifyPlan(read, true));
            AssertReadDeclaresWhatIsSent(read);
        }

        [Fact]
        public void TheImmutableFilterWithoutEmitKeepsTheFilter()
        {
            var read = Read(emit: false);
            var filter = read.Filter;

            var filterRelation = Assert.IsType<FilterRelation>(ModifyPlan(read, true));

            Assert.Same(read, filterRelation.Input);
            Assert.Same(filter, filterRelation.Condition);
            Assert.False(filterRelation.EmitSet);
            Assert.Null(read.Filter);
            AssertReadDeclaresWhatIsSent(read);
        }

        [Fact]
        public void AnImmutableReadWithoutFilterAndEmitIsLeftAlone()
        {
            var read = Read(filter: false, emit: false);

            Assert.Same(read, ModifyPlan(read, true));
            AssertReadDeclaresWhatIsSent(read);
        }
    }
}
