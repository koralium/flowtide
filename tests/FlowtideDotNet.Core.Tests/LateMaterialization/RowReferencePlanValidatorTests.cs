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
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class RowReferencePlanValidatorTests
    {
        private static ReadRelation Read(string table, string? store, params int[] stored)
        {
            return new ReadRelation()
            {
                BaseSchema = new NamedStruct() { Names = ["a", "b", "c"] },
                NamedTable = new NamedTable() { Names = [table] },
                RowStore = store == null ? null : new RowStoreDefinition() { Name = store, StoredColumns = stored.ToList() }
            };
        }

        private static FetchRequiredColumnsRelation Fetch(Relation input, int reference, string store, int[] columns, List<int>? emit = null)
        {
            return new FetchRequiredColumnsRelation()
            {
                Input = input,
                Fetches = [new RowStoreFetch() { ReferenceColumn = reference, StoreName = store, Columns = columns.ToList() }],
                Emit = emit
            };
        }

        private static WriteRelation Write(Relation input)
        {
            var names = new List<string>();
            for (int i = 0; i < input.OutputLength; i++)
            {
                names.Add("c" + i);
            }
            return new WriteRelation()
            {
                Input = input,
                NamedObject = new NamedTable() { Names = ["output"] },
                TableSchema = new NamedStruct() { Names = names }
            };
        }

        private static string Refused(params Relation[] relations)
        {
            return Assert.Throws<InvalidOperationException>(() => RowReferencePlanValidator.Validate(relations)).Message;
        }

        [Fact]
        public void APlanWithoutRowStoresPasses()
        {
            RowReferencePlanValidator.Validate([Write(new FilterRelation() { Condition = new BoolLiteral() { Value = true }, Input = Read("t", null) })]);
        }

        [Fact]
        public void AReferenceThatIsFetchedBeforeTheSinkPasses()
        {
            RowReferencePlanValidator.Validate([Write(Fetch(Read("t", "rows", 1, 2), 1, "rows", [0, 1], [0, 2, 3]))]);
        }

        [Fact]
        public void AReferenceThatReachesASinkIsRefused()
        {
            Assert.Equal(
                "A Write relation gets row references of row store 'rows' in column 1 of an input, it cannot take them. Read the stored columns with a fetch before it.",
                Refused(Write(Read("t", "rows", 1, 2))));
        }

        [Fact]
        public void AFetchThatKeepsTheReferenceDoesNotMakeItAValue()
        {
            Assert.Equal(
                "A Write relation gets row references of row store 'rows' in column 1 of an input, it cannot take them. Read the stored columns with a fetch before it.",
                Refused(Write(Fetch(Read("t", "rows", 1, 2), 1, "rows", [0, 1]))));
        }

        [Fact]
        public void AReferenceInTheRightInputOfAJoinIsRefused()
        {
            var join = new JoinRelation()
            {
                Left = Read("left", null),
                Right = Read("right", "rows", 2),
                Type = JoinType.Inner,
                Expression = new BoolLiteral() { Value = true }
            };
            Assert.StartsWith("A Join relation gets row references of row store 'rows' in column 2 of an input", Refused(join));
        }

        [Fact]
        public void ANormalizationOverAReferenceIsRefused()
        {
            var normalization = new NormalizationRelation() { KeyIndex = [0], Input = Read("t", "rows", 2) };
            Assert.StartsWith("A Normalization relation gets row references of row store 'rows' in column 2 of an input", Refused(normalization));
        }

        [Fact]
        public void ANormalizationCanHostAStore()
        {
            var normalization = new NormalizationRelation()
            {
                KeyIndex = [0],
                Input = Read("t", null),
                RowStore = new RowStoreDefinition() { Name = "normalized", StoredColumns = [1, 2] }
            };
            RowReferencePlanValidator.Validate([Write(Fetch(normalization, 1, "normalized", [1, 0], [0, 2, 3]))]);
        }

        [Fact]
        public void AFetchThroughAReferenceToAnotherRelationPasses()
        {
            var shared = Read("t", "rows", 1, 2);
            var reference = new ReferenceRelation() { RelationId = 1, ReferenceOutputLength = 2 };
            RowReferencePlanValidator.Validate([Write(Fetch(reference, 1, "rows", [0, 1], [0, 2, 3])), shared]);
        }

        [Fact]
        public void ASinkOverAReferenceToAStoreRelationIsRefused()
        {
            var reference = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 2 };
            Assert.Equal(
                "A Write relation gets row references of row store 'rows' in column 1 of an input, it cannot take them. Read the stored columns with a fetch before it.",
                Refused(Read("t", "rows", 1, 2), Write(reference)));
        }

        [Fact]
        public void AnEmitOnAReferenceToAStoreRelationIsRefused()
        {
            var reference = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 2, Emit = [0] };
            Assert.Equal(
                "A reference to relation 0 has an emit and that relation gives row references, the emit is not applied when the stream runs.",
                Refused(Read("t", "rows", 1, 2), Write(reference)));
        }

        [Fact]
        public void AReferenceWithAnotherWidthThanItsStoreRelationIsRefused()
        {
            var reference = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 3 };
            Assert.Equal(
                "A reference to relation 0 says it has 3 columns, that relation gives 2 with its row references.",
                Refused(Read("t", "rows", 1, 2), Write(reference)));
        }

        [Fact]
        public void AnEmitOnAReferenceToARelationWithoutStoresPasses()
        {
            var reference = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 3, Emit = [2, 0] };
            RowReferencePlanValidator.Validate([Read("t", null), Write(reference)]);
        }

        [Fact]
        public void RelationsThatReferenceEachOtherAreWalkedOnce()
        {
            var first = new FilterRelation() { Condition = new BoolLiteral() { Value = true }, Input = new ReferenceRelation() { RelationId = 1, ReferenceOutputLength = 3 } };
            var second = new FilterRelation() { Condition = new BoolLiteral() { Value = true }, Input = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 3 } };
            RowReferencePlanValidator.Validate([first, second, Write(Fetch(Read("t", "rows", 1, 2), 1, "rows", [0, 1], [0, 2, 3]))]);
        }

        [Fact]
        public void TwoFetchesThroughOneReferenceColumnAreRefused()
        {
            var fetch = new FetchRequiredColumnsRelation()
            {
                Input = Read("t", "rows", 1, 2),
                Fetches =
                [
                    new RowStoreFetch() { ReferenceColumn = 1, StoreName = "rows", Columns = [0] },
                    new RowStoreFetch() { ReferenceColumn = 1, StoreName = "rows", Columns = [1] }
                ]
            };
            Assert.Equal("Two fetches read through reference column 1, name every column of a store in one fetch.", Refused(fetch));
        }

        [Fact]
        public void AReferenceInASetOperationOrALoopIsRefused()
        {
            var set = new SetRelation() { Inputs = [Read("t", "rows", 2), Read("u", "other", 2)], Operation = SetOperation.MinusPrimary };
            Assert.StartsWith("A Set relation gets row references of row store 'rows' in column 2 of an input", Refused(set));

            var iteration = new IterationRelation()
            {
                IterationName = "loop",
                Input = Read("t", "rows", 2),
                LoopPlan = new IterationReferenceReadRelation() { IterationName = "loop", ReferenceOutputLength = 3 }
            };
            Assert.StartsWith("A Iteration relation gets row references of row store 'rows' in column 2 of an input", Refused(iteration));
        }

        [Theory]
        [InlineData(5, "rows", 0, "A fetch from row store 'rows' reads its references from column 5, the input has 2.")]
        [InlineData(0, "rows", 0, "A fetch from row store 'rows' reads its references from column 0, that column holds no row references.")]
        [InlineData(1, "other", 0, "A fetch reads from row store 'other', no relation of the plan hosts it.")]
        [InlineData(1, "rows", 2, "A fetch asks for stored column 2 of row store 'rows', it stores 2.")]
        [InlineData(1, "rows", -1, "A fetch asks for stored column -1 of row store 'rows', it stores 2.")]
        public void AWrongFetchIsRefused(int reference, string store, int column, string message)
        {
            Assert.Equal(message, Refused(Write(Fetch(Read("t", "rows", 1, 2), reference, store, [column], [0, 2]))));
        }

        [Fact]
        public void AFetchWithTheReferencesOfAnotherStoreIsRefused()
        {
            var join = new FetchRequiredColumnsRelation()
            {
                Input = Read("t", "rows", 1, 2),
                Fetches = [new RowStoreFetch() { ReferenceColumn = 1, StoreName = "second", Columns = [0] }]
            };
            Assert.Equal(
                "A fetch from row store 'second' reads its references from column 1, that column holds references of row store 'rows'.",
                Refused(Write(join), Read("u", "second", 2)));
        }

        [Fact]
        public void AFetchWithoutColumnsIsRefused()
        {
            Assert.Equal("A fetch from row store 'rows' reads no column.", Refused(Fetch(Read("t", "rows", 1, 2), 1, "rows", [])));
        }

        [Theory]
        [InlineData(new int[] { 3 }, "Row store 'rows' stores output column 3, the relation has 3.")]
        [InlineData(new int[] { -1 }, "Row store 'rows' stores output column -1, the relation has 3.")]
        [InlineData(new int[] { 1, 1 }, "Row store 'rows' names output column 1 twice.")]
        [InlineData(new int[] { }, "Row store 'rows' stores no column.")]
        public void AWrongRowStoreIsRefused(int[] stored, string message)
        {
            Assert.Equal(message, Refused(Read("t", "rows", stored)));
        }

        [Fact]
        public void ARowStoreWithoutANameIsRefused()
        {
            Assert.Equal("A Read relation has a row store without a name.", Refused(Read("t", "", 1)));
        }

        [Fact]
        public void TwoRelationsWithOneStoreNameAreRefused()
        {
            Assert.Equal("Row store 'rows' is hosted by more than one relation.", Refused(Read("t", "rows", 1), Read("u", "rows", 2)));
        }
    }
}
