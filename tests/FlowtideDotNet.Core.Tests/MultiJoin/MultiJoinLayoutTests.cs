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

using FlowtideDotNet.Base;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinLayoutTests
    {
        private const string TenantTables = @"
            CREATE TABLE orders (id any, tenant any, user_id any, product any);
            CREATE TABLE tenants (id any, name any);
            CREATE TABLE users (id any, name any);
            CREATE TABLE products (tenant any, id any, name any);
        ";

        private static ReadRelation Read(string name, int columns)
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { name } },
                BaseSchema = new NamedStruct()
                {
                    Names = Enumerable.Range(0, columns).Select(x => "c" + x).ToList(),
                    Struct = new Struct() { Types = Enumerable.Range(0, columns).Select(x => (SubstraitBaseType)new AnyType()).ToList() }
                }
            };
        }

        private static MergeJoinRelation Join(JoinType type, Relation left, Relation right, int leftKey, int rightKey)
        {
            return new MergeJoinRelation()
            {
                Type = type,
                Left = left,
                Right = right,
                LeftKeys = new List<FieldReference>() { new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = leftKey } } },
                RightKeys = new List<FieldReference>() { new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = rightKey } } }
            };
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void NestedKeyChainLeadsThePrimaryTreeWhateverTheJoinOrder(bool tenantsFirst)
        {
            var joins = tenantsFirst
                ? "INNER JOIN tenants t ON o.tenant = t.id INNER JOIN users u ON o.user_id = u.id"
                : "INNER JOIN users u ON o.user_id = u.id INNER JOIN tenants t ON o.tenant = t.id";
            var optimized = MultiJoinPlanBuilderTests.Optimized($@"
                INSERT INTO output
                SELECT o.id, t.name, u.name, p.name
                FROM orders o
                {joins}
                INNER JOIN products p ON o.tenant = p.tenant AND o.product = p.id;", TenantTables);
            var plan = MultiJoinPlanBuilder.Discover(MultiJoinPlanBuilderTests.RootJoin(optimized), 5)!;
            Assert.NotNull(plan);

            // orders is looked up by {tenant}, {user_id} and {tenant, product}: one primary order serves the first and last
            Assert.Single(plan.Inputs[0].SecondaryLayouts);
        }

        [Fact]
        public void KeySetsAreServedByTheFewestTrees()
        {
            var optimized = MultiJoinPlanBuilderTests.Optimized(@"
                INSERT INTO output
                SELECT o.id, t.name, u.name, a.name, p.name
                FROM orders o
                INNER JOIN tenants t ON o.tenant = t.id
                INNER JOIN users u ON o.user_id = u.id
                INNER JOIN accounts a ON o.tenant = a.tenant AND o.user_id = a.user_id
                INNER JOIN products p ON o.tenant = p.tenant AND o.product = p.id;", TenantTables + "CREATE TABLE accounts (tenant any, user_id any, name any);");
            var plan = MultiJoinPlanBuilder.Discover(MultiJoinPlanBuilderTests.RootJoin(optimized), 5)!;
            Assert.NotNull(plan);
            Assert.Equal(5, plan.Inputs.Count);

            // orders is looked up by {tenant}, {user_id}, {tenant, user_id} and {tenant, product}: [tenant, product] and [user_id, tenant] serve all four
            Assert.Single(plan.Inputs[0].SecondaryLayouts);
        }

        [Fact]
        public void ReferenceToTheBodyOfAnotherLoopFromInsideALoopIsALoopBody()
        {
            var plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    // Loop b runs relation 1
                    new IterationRelation() { IterationName = "b", LoopPlan = new ReferenceRelation() { RelationId = 1, ReferenceOutputLength = 2 } },
                    // Relation 1 owns loop a, whose plan is relation 2
                    new IterationRelation() { IterationName = "a", LoopPlan = new ReferenceRelation() { RelationId = 2, ReferenceOutputLength = 2 } },
                    // Relation 2 reads loop b
                    new IterationReferenceReadRelation() { IterationName = "b", ReferenceOutputLength = 2 }
                }
            };

            var bodies = MultiJoinLoopBodies.Find(plan);

            Assert.Contains(2, bodies);
            // Relation 1 reads loop b through relation 2 and does not own it
            Assert.Contains(1, bodies);
            // Relation 0 owns the loops it reads
            Assert.DoesNotContain(0, bodies);
        }

        [Fact]
        public async Task PadKeysTakenFromAViewAreCopiedWithoutAnObjectPerRow()
        {
            using var host = new MultiJoinAllocationTests.Host();
            // a left join b on a.k = b.k, then left join c on b.j = c.j: a change on b enters a and remembers its keys
            var ab = Join(JoinType.Left, Read("a", 2), Read("b", 3), 0, 2);
            var plan = MultiJoinPlanBuilder.Discover(Join(JoinType.Left, ab, Read("c", 2), 3, 5), 9);
            Assert.NotNull(plan);
            var op = new MultiJoinOperator(plan, host.FunctionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            op.LinkTo(new BufferBlock<IStreamEvent>());
            await host.InitializeOperator(op);
            var table = op.PadTables[0];

            const int rows = 512;
            var allocator = GlobalMemoryManager.Instance;
            var plain = Column.Create(allocator);
            var inner = Column.Create(allocator);
            for (int i = 0; i < rows * 2; i++)
            {
                inner.Add(new Int64Value(i));
            }
            var offsets = new PrimitiveList<int>(allocator);
            for (int i = 0; i < rows; i++)
            {
                plain.Add(new Int64Value(i * 2));
                offsets.Add(i * 2);
            }
            // Every other row of a batch, as a filter hands it on
            var view = new ColumnWithOffset(inner, offsets);
            var found = Enumerable.Repeat(true, rows).ToArray();
            var weights = Enumerable.Repeat(1, rows).ToArray();

            async Task<long> Measure(IColumn keys)
            {
                var columns = new IColumn[] { keys };
                // The table's own columns are made on the first key of a change, both runs pay that
                await table.Add(columns, rows, found, weights);
                table.Abandon();
                long before = GC.GetAllocatedBytesForCurrentThread();
                await table.Add(columns, rows, found, weights);
                long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
                table.Abandon();
                return allocated;
            }

            long fromColumn = await Measure(plain);
            long fromView = await Measure(view);
            Assert.True(fromView <= fromColumn + rows * 4, $"Bytes allocated remembering {rows} keys: from a column {fromColumn}, from a view {fromView}");
        }

        [Fact]
        public async Task PadKeysFromUnionOffsetViewWithNullOffsetCanBeGathered()
        {
            using var host = new MultiJoinAllocationTests.Host();
            var ab = Join(JoinType.Left, Read("a", 2), Read("b", 3), 0, 2);
            var plan = MultiJoinPlanBuilder.Discover(Join(JoinType.Left, ab, Read("c", 2), 3, 5), 9);
            Assert.NotNull(plan);
            var op = new MultiJoinOperator(plan, host.FunctionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            try
            {
                op.LinkTo(new BufferBlock<IStreamEvent>());
                await host.InitializeOperator(op);
                var table = op.PadTables[0];

                var allocator = GlobalMemoryManager.Instance;
                using var inner = Column.Create(allocator);
                inner.Add(new Int64Value(10));
                inner.Add(new FlowtideDotNet.Core.ColumnStore.StringValue("twenty"));

                // NullValueIndex (-1) followed by 0 exercises contiguous-run detection in Gather
                using var offsets = new PrimitiveList<int>(allocator);
                offsets.Add(ColumnWithOffset.NullValueIndex);
                offsets.Add(0);

                var view = new ColumnWithOffset(inner, offsets);
                await table.Add(new IColumn[] { view }, 2, new[] { true, true }, new[] { 1, 1 });
                Assert.Equal(2, table.KeysAdded);
                table.Abandon();
            }
            finally
            {
                await op.DisposeAsync();
            }
        }
    }
}
