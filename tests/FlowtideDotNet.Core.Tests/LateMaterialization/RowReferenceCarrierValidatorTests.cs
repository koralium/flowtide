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

using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class RowReferenceCarrierValidatorTests
    {
        // Columns a, b and a reference when stored
        private static ReadRelation Read(string table, string? store)
        {
            return new ReadRelation()
            {
                BaseSchema = new NamedStruct() { Names = ["a", "b", "c"] },
                NamedTable = new NamedTable() { Names = [table] },
                RowStore = store == null ? null : new RowStoreDefinition() { Name = store, StoredColumns = [2] }
            };
        }

        private static DirectFieldReference Field(int field)
        {
            return new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = field } };
        }

        private static ScalarFunction Equal(int left, int right)
        {
            return new ScalarFunction()
            {
                ExtensionUrn = FunctionsComparison.Urn,
                ExtensionName = FunctionsComparison.Equal,
                Arguments = [Field(left), Field(right)]
            };
        }

        private static FetchRequiredColumnsRelation Fetch(Relation input, params (int reference, string store)[] fetches)
        {
            return new FetchRequiredColumnsRelation()
            {
                Input = input,
                Fetches = fetches.Select(x => new RowStoreFetch() { ReferenceColumn = x.reference, StoreName = x.store, Columns = [0] }).ToList()
            };
        }

        private static WriteRelation Write(Relation input, params int[] emit)
        {
            input.Emit = emit.ToList();
            return new WriteRelation()
            {
                Input = input,
                NamedObject = new NamedTable() { Names = ["output"] },
                TableSchema = new NamedStruct() { Names = emit.Select(x => "c" + x).ToList() }
            };
        }

        private static MergeJoinRelation Join(Relation left, Relation right, JoinType type = JoinType.Inner)
        {
            return new MergeJoinRelation()
            {
                Left = left,
                Right = right,
                Type = type,
                LeftKeys = [Field(0)],
                RightKeys = [Field(left.OutputLength)]
            };
        }

        private static string Refused(params Relation[] relations)
        {
            return Assert.Throws<InvalidOperationException>(() => RowReferencePlanValidator.Validate(relations)).Message;
        }

        [Fact]
        public void AFilterOnKeptColumnsPassesReferencesOn()
        {
            var filter = new FilterRelation() { Condition = Equal(0, 1), Input = Read("t", "rows") };
            RowReferencePlanValidator.Validate([Write(Fetch(filter, (2, "rows")), 0, 1, 3)]);
        }

        [Fact]
        public void AFilterThatReadsAReferenceIsRefused()
        {
            var filter = new FilterRelation() { Condition = Equal(0, 2), Input = Read("t", "rows") };
            Assert.Equal(
                "A Filter relation reads column 2 in its condition, that column holds row references of row store 'rows' and a reference is not the value. Fetch the stored columns before it.",
                Refused(Write(Fetch(filter, (2, "rows")), 0, 1, 3)));
        }

        [Fact]
        public void AFilterEmitMovesTheReference()
        {
            var filter = new FilterRelation() { Condition = Equal(0, 1), Input = Read("t", "rows"), Emit = [2, 0] };
            RowReferencePlanValidator.Validate([Write(Fetch(filter, (0, "rows")), 1, 2)]);
            Assert.StartsWith("A fetch from row store 'rows' reads its references from column 1, that column holds no row references.", Refused(Write(Fetch(filter, (1, "rows")), 1, 2)));
        }

        [Fact]
        public void AProjectPassesAReferenceOnAndCanNameItAgain()
        {
            var project = new ProjectRelation() { Input = Read("t", "rows"), Expressions = [Equal(0, 1), Field(2)], Emit = [0, 3, 4] };
            RowReferencePlanValidator.Validate([Write(Fetch(project, (2, "rows")), 0, 1, 3)]);
            Assert.StartsWith("A fetch from row store 'rows' reads its references from column 1, that column holds no row references.", Refused(Write(Fetch(project, (1, "rows")), 0, 1, 3)));
        }

        [Fact]
        public void AProjectThatComputesOnAReferenceIsRefused()
        {
            var project = new ProjectRelation() { Input = Read("t", "rows"), Expressions = [Equal(2, 1)] };
            Assert.StartsWith("A Project relation reads column 2 in its expressions, that column holds row references of row store 'rows'", Refused(project));

            var nested = new ProjectRelation()
            {
                Input = Read("t", "rows"),
                Expressions = [new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = 2, Child = new StructReferenceSegment() { Field = 0 } } }]
            };
            Assert.StartsWith("A Project relation reads column 2 in its expressions", Refused(nested));
        }

        [Fact]
        public void AFunctionThatWorksOnWholeRowsIsRefusedNextToAReference()
        {
            var functions = new FunctionsRegister();
            functions.RegisterScalarFunction("extension:test:rows", "rows_only", (function, parameters, visitor) => System.Linq.Expressions.Expression.Constant(default(FlexBuffers.FlxValue)));
            var call = new ScalarFunction() { ExtensionUrn = "extension:test:rows", ExtensionName = "rows_only", Arguments = [Field(0)] };
            var filter = new FilterRelation() { Condition = call, Input = Read("t", "rows") };

            var exception = Assert.Throws<InvalidOperationException>(() => RowReferencePlanValidator.Validate([Write(Fetch(filter, (2, "rows")), 0, 1, 3)], functions));
            Assert.Equal(
                "A Filter relation uses function 'rows_only' in its condition next to row references and the function has no columnar form, it would work on whole rows and a row reference cannot be turned into one. Fetch every stored column before it.",
                exception.Message);

            var plain = new FilterRelation() { Condition = call, Input = Read("t", null) };
            RowReferencePlanValidator.Validate([Write(plain, 0, 1, 2)], functions);
            var fetch = Fetch(Read("t", "rows"), (2, "rows"));
            fetch.Emit = [0, 1, 3];
            RowReferencePlanValidator.Validate([Write(new FilterRelation() { Condition = call, Input = fetch }, 0, 1, 2)], functions);
        }

        [Theory]
        [InlineData(JoinType.Inner)]
        [InlineData(JoinType.Left)]
        [InlineData(JoinType.Right)]
        [InlineData(JoinType.Outer)]
        public void AMergeJoinPassesTheReferencesOfBothSidesOn(JoinType type)
        {
            var join = Join(Read("l", "left"), Read("r", "right"), type);
            RowReferencePlanValidator.Validate([Write(Fetch(join, (2, "left"), (5, "right")), 0, 1, 3, 4, 6, 7)]);
            Assert.StartsWith("A fetch from row store 'left' reads its references from column 5, that column holds references of row store 'right'.", Refused(Write(Fetch(join, (5, "left")), 0, 1, 3, 4, 6)));
        }

        [Fact]
        public void AMarkJoinKeepsOnlyTheLeftReferences()
        {
            var join = Join(Read("l", "left"), Read("r", "right"), JoinType.LeftMark);
            RowReferencePlanValidator.Validate([Write(Fetch(join, (2, "left")), 0, 1, 3, 4)]);
            Assert.StartsWith("A fetch from row store 'right' reads its references from column 3, that column holds no row references.", Refused(Write(Fetch(join, (3, "right")), 0, 1, 4)));
        }

        [Fact]
        public void AJoinOnAReferenceOrWithAConditionOnOneIsRefused()
        {
            var keyed = Join(Read("l", "left"), Read("r", null));
            keyed.LeftKeys = [Field(2)];
            Assert.StartsWith("A MergeJoin relation reads column 2 in its keys, that column holds row references of row store 'left'", Refused(keyed));

            var filtered = Join(Read("l", null), Read("r", "right"));
            filtered.PostJoinFilter = Equal(1, 5);
            Assert.StartsWith("A MergeJoin relation reads column 5 in its condition, that column holds row references of row store 'right'", Refused(filtered));
        }

        [Fact]
        public void AMergeJoinUnderBuilderParallelismIsRefused()
        {
            var join = Join(Read("l", "left"), Read("r", null));
            var exception = Assert.Throws<InvalidOperationException>(() => RowReferencePlanValidator.Validate([Write(Fetch(join, (2, "left")), 0, 1, 3, 4, 5, 6)], null, 2));
            Assert.StartsWith("A merge join gets row references and the stream runs with parallelism set on the builder", exception.Message);

            join.MultiJoinGroup = 1;
            RowReferencePlanValidator.Validate([Write(Fetch(join, (2, "left")), 0, 1, 3, 4, 5, 6)], null, 2);
        }

        [Fact]
        public void APlanWithAStoreNeedsTheColumnStore()
        {
            var exception = Assert.Throws<InvalidOperationException>(() => RowReferencePlanValidator.Validate([Write(Fetch(Read("t", "rows"), (2, "rows")), 0, 1, 3)], null, 1, false));
            Assert.Equal("A plan with a row store needs the column store, the row operators cannot take row references.", exception.Message);
            RowReferencePlanValidator.Validate([Write(Read("t", null), 0, 1, 2)], null, 1, false);
        }

        [Fact]
        public void AUnionPassesReferencesOfOneStoreOn()
        {
            var same = new SetRelation() { Inputs = [Read("t", "rows"), new FilterRelation() { Condition = Equal(0, 1), Input = Read("t2", "rows2") }], Operation = SetOperation.UnionAll };
            Assert.Equal(
                "A union gets row references of row store 'rows' in column 2 of input 0 and row references of row store 'rows2' in column 2 of input 1, one fetch must be able to read the column. Fetch the stored columns before the union.",
                Refused(same));

            var mixed = new SetRelation() { Inputs = [Read("t", "rows"), Read("u", null)], Operation = SetOperation.UnionAll };
            Assert.StartsWith("A union gets row references of row store 'rows' in column 2 of input 0 and values in column 2 of input 1", Refused(mixed));
        }

        // A scatter into two partitions that are joined and merged again
        private static List<Relation> Partitioned(int scatterField, ExchangeTarget? extraTarget = null)
        {
            var targets = new List<ExchangeTarget>()
            {
                new StandardOutputExchangeTarget() { PartitionIds = [0] },
                new StandardOutputExchangeTarget() { PartitionIds = [1] }
            };
            if (extraTarget != null)
            {
                targets.Add(extraTarget);
            }
            var exchange = new ExchangeRelation()
            {
                Input = Read("t", "rows"),
                ExchangeKind = new ScatterExchangeKind() { Fields = [Field(scatterField)] },
                PartitionCount = 2,
                Targets = targets
            };
            var union = new SetRelation()
            {
                Operation = SetOperation.UnionAll,
                Inputs =
                [
                    new FilterRelation() { Condition = Equal(0, 1), Input = new StandardOutputExchangeReferenceRelation() { RelationId = 0, TargetId = 0, ReferenceOutputLength = 3 } },
                    new FilterRelation() { Condition = Equal(0, 1), Input = new StandardOutputExchangeReferenceRelation() { RelationId = 0, TargetId = 1, ReferenceOutputLength = 3 } }
                ]
            };
            return [exchange, Write(Fetch(union, (2, "rows")), 0, 1, 3)];
        }

        [Fact]
        public void ALocalScatterAndItsUnionPassReferencesOn()
        {
            RowReferencePlanValidator.Validate(Partitioned(0));
        }

        [Fact]
        public void AScatterOnAReferenceIsRefused()
        {
            Assert.StartsWith("A Exchange relation reads column 2 in its scatter fields, that column holds row references of row store 'rows'", Refused(Partitioned(2).ToArray()));
        }

        [Fact]
        public void AnExchangeOutOfTheStreamIsRefused()
        {
            Assert.Equal(
                "An exchange gets row references of row store 'rows' in column 2 and sends them out of the stream, a reference is only good next to its store. Fetch the stored columns before it.",
                Refused(Partitioned(0, new SubstreamExchangeTarget() { SubstreamName = "other", PartitionIds = [2] }).ToArray()));
            Assert.StartsWith("An exchange gets row references of row store 'rows' in column 2 and sends them out of the stream", Refused(Partitioned(0, new PullBucketExchangeTarget() { PartitionIds = [2] }).ToArray()));
        }

        [Fact]
        public void AnExchangeOutputWithAWrongWidthIsRefused()
        {
            var relations = Partitioned(0);
            var union = (SetRelation)((FetchRequiredColumnsRelation)((WriteRelation)relations[1]).Input).Input;
            ((StandardOutputExchangeReferenceRelation)((FilterRelation)union.Inputs[1]).Input).ReferenceOutputLength = 2;
            Assert.StartsWith("A reference to exchange 0 says it has 2 columns, the exchange gives 3 with its row references.", Refused(relations.ToArray()));
        }

        [Fact]
        public void BoundariesStillRefuseReferences()
        {
            var read = Read("t", "rows");
            Assert.StartsWith("A Join relation gets row references", Refused(new JoinRelation() { Left = read, Right = Read("u", null), Type = JoinType.Inner, Expression = new BoolLiteral() { Value = true } }));
            Assert.StartsWith("A Aggregate relation gets row references", Refused(new AggregateRelation() { Input = Read("t", "rows") }));
            Assert.StartsWith("A TopN relation gets row references", Refused(new TopNRelation() { Input = Read("t", "rows"), Count = 1, Sorts = [] }));
            Assert.StartsWith("A Buffer relation gets row references", Refused(new BufferRelation() { Input = Read("t", "rows") }));
            Assert.StartsWith("A Write relation gets row references", Refused(Write(Read("t", "rows"), 0, 1, 2)));
            Assert.StartsWith("A Set relation gets row references", Refused(new SetRelation() { Inputs = [Read("t", "rows")], Operation = SetOperation.UnionDistinct }));
        }

        [Fact]
        public void AMarkJoinGivesTheLeftColumnsAndOneValue()
        {
            var join = Join(Read("l", "left"), Read("r", "right"), JoinType.LeftMark);
            Assert.StartsWith("A fetch from row store 'right' reads its references from column 5, the input has 4.", Refused(Write(Fetch(join, (5, "right")), 0, 1, 3)));
        }

        [Fact]
        public void AReferenceAsARightKeyIsRefused()
        {
            var join = Join(Read("l", null), Read("r", "right"));
            join.RightKeys = [Field(5)];
            Assert.StartsWith("A MergeJoin relation reads column 5 in its keys, that column holds row references of row store 'right'", Refused(join));
        }

        [Fact]
        public void AFunctionWithoutAColumnarFormIsRefusedInEveryCarrier()
        {
            var functions = new FunctionsRegister();
            Core.Compute.Internal.BuiltinFunctions.RegisterFunctions(functions);
            functions.RegisterScalarFunction("extension:test:rows", "rows_only", (function, parameters, visitor) => System.Linq.Expressions.Expression.Constant(default(FlexBuffers.FlxValue)));
            var rowsOnly = new ScalarFunction() { ExtensionUrn = "extension:test:rows", ExtensionName = "rows_only", Arguments = [Field(0)] };
            var unknown = new ScalarFunction() { ExtensionUrn = "extension:test:rows", ExtensionName = "registered_later", Arguments = [Field(0)] };
            string RefusedWith(Relation relation)
            {
                return Assert.Throws<InvalidOperationException>(() => RowReferencePlanValidator.Validate([relation], functions)).Message;
            }

            Assert.StartsWith("A Project relation uses function 'rows_only' in its expressions", RefusedWith(new ProjectRelation() { Input = Read("t", "rows"), Expressions = [rowsOnly] }));
            var join = Join(Read("l", "left"), Read("r", null));
            join.PostJoinFilter = rowsOnly;
            Assert.StartsWith("A MergeJoin relation uses function 'rows_only' in its condition", RefusedWith(join));
            Assert.StartsWith("A Filter relation uses function 'registered_later' in its condition", RefusedWith(new FilterRelation() { Input = Read("t", "rows"), Condition = unknown }));
            RowReferencePlanValidator.Validate([Write(new FilterRelation() { Input = Read("t", null), Condition = unknown }, 0, 1, 2)], functions);
        }

        [Fact]
        public void TheBuiltInFunctionsAreAssumedWhenNoRegisterIsGiven()
        {
            var unknown = new ScalarFunction() { ExtensionUrn = "extension:test:rows", ExtensionName = "nowhere", Arguments = [Field(0)] };
            Assert.StartsWith("A Filter relation uses function 'nowhere' in its condition", Refused(new FilterRelation() { Input = Read("t", "rows"), Condition = unknown }));
        }

        private static ExchangeRelation Exchange(ExchangeKind kind)
        {
            return new ExchangeRelation() { Input = Read("t", "rows"), ExchangeKind = kind, Targets = [new StandardOutputExchangeTarget() { PartitionIds = [0] }] };
        }

        [Fact]
        public void ABroadcastPassesReferencesOn()
        {
            var output = new StandardOutputExchangeReferenceRelation() { RelationId = 0, TargetId = 0, ReferenceOutputLength = 3 };
            RowReferencePlanValidator.Validate([Exchange(new BroadcastExchangeKind()), Write(Fetch(output, (2, "rows")), 0, 1, 3)]);
        }

        [Fact]
        public void AnExchangeOfAnotherKindIsRefused()
        {
            Assert.Equal(
                "An exchange of kind SingleBucket gets row references of row store 'rows' in column 2, only scatter and broadcast exchanges pass them on.",
                Refused(Exchange(new SingleBucketExchangeKind() { Expression = Field(0) })));
        }

        [Fact]
        public void AnEmitOnAnExchangeOrItsOutputIsRefused()
        {
            var exchange = Exchange(new BroadcastExchangeKind());
            exchange.Emit = [0, 1];
            Assert.Equal(
                "An exchange gets row references of row store 'rows' in column 2 and has an emit, the emit is not applied when the stream runs.",
                Refused(exchange));

            var output = new StandardOutputExchangeReferenceRelation() { RelationId = 0, TargetId = 0, ReferenceOutputLength = 3, Emit = [0, 1] };
            Assert.Equal(
                "A reference to exchange 0 has an emit and that exchange gives row references, the emit is not applied when the stream runs.",
                Refused(Exchange(new BroadcastExchangeKind()), Write(output, 0, 1)));
        }

        [Fact]
        public void ASubqueryNextToAReferenceIsRefused()
        {
            var filter = new FilterRelation()
            {
                Input = Read("t", "rows"),
                Condition = new SetPredicateExpression() { Relation = Read("u", null) }
            };
            Assert.Equal("A Filter relation has a subquery in its condition next to row references. Fetch the stored columns before it.", Refused(filter));
        }

        [Fact]
        public void AnEmitOnAUnionOrAJoinMovesTheReference()
        {
            var union = new SetRelation()
            {
                Operation = SetOperation.UnionAll,
                Inputs = [new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 3 }, new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 3 }],
                Emit = [2, 0]
            };
            RowReferencePlanValidator.Validate([Read("t", "rows"), Write(Fetch(union, (0, "rows")), 1, 2)]);
            Assert.StartsWith("A fetch from row store 'rows' reads its references from column 2, the input has 2.", Refused(Read("t", "rows"), Write(Fetch(union, (2, "rows")), 1, 2)));

            var join = Join(Read("l", "left"), Read("r", "right"));
            join.Emit = [5, 0, 2];
            RowReferencePlanValidator.Validate([Write(Fetch(join, (0, "right"), (2, "left")), 1, 3, 4)]);
            Assert.StartsWith("A fetch from row store 'left' reads its references from column 0, that column holds references of row store 'right'.", Refused(Write(Fetch(join, (0, "left")), 1, 3)));
        }
    }
}
