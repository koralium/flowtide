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

using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    public class MultiJoinNestedLoopTests
    {
        private static VirtualTableReadRelation Input()
        {
            return new VirtualTableReadRelation()
            {
                BaseSchema = new NamedStruct()
                {
                    Names = new List<string>() { "k", "v" },
                    Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType(), new AnyType() } }
                },
                Values = new VirtualTable() { Expressions = new List<StructExpression>() }
            };
        }

        private static MergeJoinRelation Join(Relation left, Relation right)
        {
            return new MergeJoinRelation()
            {
                Type = JoinType.Left,
                Left = left,
                Right = right,
                LeftKeys = new List<FieldReference>()
                {
                    new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = 0 } }
                },
                RightKeys = new List<FieldReference>()
                {
                    new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = left.OutputLength } }
                }
            };
        }

        private static (Plan Plan, MergeJoinRelation Lower, MergeJoinRelation Upper) NestedLoopPlan()
        {
            var nested = new IterationRelation()
            {
                IterationName = "inner",
                Input = Input(),
                MaxIterations = 2,
                LoopPlan = new SetRelation()
                {
                    Operation = SetOperation.UnionAll,
                    Inputs = new List<Relation>()
                    {
                        new IterationReferenceReadRelation() { IterationName = "inner", ReferenceOutputLength = 2 },
                        new ReferenceRelation() { RelationId = 2, ReferenceOutputLength = 2 }
                    }
                }
            };
            var lower = Join(nested, Input());
            var upper = Join(lower, Input());
            upper.Emit = new List<int>() { 0, 1 };
            var outer = new IterationRelation()
            {
                IterationName = "outer",
                Input = Input(),
                MaxIterations = 2,
                LoopPlan = new ReferenceRelation() { RelationId = 1, ReferenceOutputLength = 2 }
            };
            var outerRead = new BufferRelation()
            {
                Input = new IterationReferenceReadRelation() { IterationName = "outer", ReferenceOutputLength = 2 }
            };
            return (new Plan() { Relations = new List<Relation>() { outer, upper, outerRead } }, lower, upper);
        }

        [Fact]
        public void NestedOwnedLoopDoesNotHideDependencyOnAnOuterLoop()
        {
            var (plan, _, upper) = NestedLoopPlan();

            Assert.Contains(1, MultiJoinLoopBodies.Find(plan));
            Assert.Empty(MultiJoinReport.Create(plan, 5).Groups);
            Assert.Equal(0, MultiJoinMarker.Mark(plan, 5));
            Assert.Null(upper.MultiJoinGroup);
        }

        [Fact]
        public void MarkedIndirectNestedLoopBodyIsRejectedByEngine()
        {
            var (plan, lower, upper) = NestedLoopPlan();
            lower.MultiJoinGroup = 1;
            upper.MultiJoinGroup = 1;
            var functions = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(functions);
            var visitor = new SubstraitVisitor(
                plan,
                new DataflowStreamBuilder("nested_loop"),
                null,
                null,
                100,
                functions,
                1,
                TimeSpan.FromSeconds(1),
                true,
                NullLoggerFactory.Instance);

            var error = Assert.Throws<NotSupportedException>(() => visitor.BuildPlan());
            Assert.Contains("multi join cannot run inside a loop", error.Message);
        }

        [Fact]
        public void LoopBodyReadThroughStandardOutputExchangeReferenceIsDetectedAndRejected()
        {
            var exchange = new ExchangeRelation()
            {
                Input = new IterationReferenceReadRelation() { IterationName = "outer", ReferenceOutputLength = 2 },
                ExchangeKind = new BroadcastExchangeKind(),
                Targets = new List<ExchangeTarget>() { new StandardOutputExchangeTarget() { PartitionIds = new List<int>() } }
            };
            var lower = Join(
                new StandardOutputExchangeReferenceRelation() { RelationId = 2, TargetId = 0, ReferenceOutputLength = 2 },
                Input());
            var upper = Join(lower, Input());
            upper.Emit = new List<int>() { 0, 1 };
            var outer = new IterationRelation()
            {
                IterationName = "outer",
                Input = Input(),
                MaxIterations = 2,
                LoopPlan = new ReferenceRelation() { RelationId = 1, ReferenceOutputLength = 2 }
            };
            var plan = new Plan() { Relations = new List<Relation>() { outer, upper, exchange } };

            Assert.Contains(1, MultiJoinLoopBodies.Find(plan));
            Assert.Empty(MultiJoinReport.Create(plan, 5).Groups);
            Assert.Equal(0, MultiJoinMarker.Mark(plan, 5));

            lower.MultiJoinGroup = 1;
            upper.MultiJoinGroup = 1;
            var functions = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(functions);
            var visitor = new SubstraitVisitor(
                plan,
                new DataflowStreamBuilder("exchange_loop"),
                null,
                null,
                100,
                functions,
                1,
                TimeSpan.FromSeconds(1),
                true,
                NullLoggerFactory.Instance);

            var error = Assert.Throws<NotSupportedException>(() => visitor.BuildPlan());
            Assert.Contains("multi join cannot run inside a loop", error.Message);
        }
    }
}
