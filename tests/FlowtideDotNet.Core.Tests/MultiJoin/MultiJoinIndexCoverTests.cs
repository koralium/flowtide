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

using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    public class MultiJoinIndexCoverTests
    {
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

        private static MergeJoinRelation Join(Relation left, Relation right, params (int Left, int Right)[] keys)
        {
            return new MergeJoinRelation()
            {
                Type = JoinType.Inner,
                Left = left,
                Right = right,
                LeftKeys = keys.Select(x => (FieldReference)new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = x.Left } }).ToList(),
                RightKeys = keys.Select(x => (FieldReference)new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = x.Right } }).ToList()
            };
        }

        [Fact]
        public void BranchingNestedKeySetsShareOneSecondaryTree()
        {
            var first = Join(Read("f", 3), Read("a", 2), (0, 3));
            var second = Join(first, Read("b", 2), (1, 5));
            var third = Join(second, Read("c", 3), (0, 7), (1, 8));
            var root = Join(third, Read("d", 3), (0, 10), (2, 11));

            var plan = MultiJoinPlanBuilder.Discover(root, 5);

            Assert.NotNull(plan);
            Assert.Equal(5, plan.Inputs.Count);
            var fact = plan.Inputs[0];
            Assert.Equal(4, fact.KeyLists.Count);
            // The primary tree [0, 2, 1] serves {0} and {0, 2}, one secondary [1, 0, 2] serves {1} and {0, 1}
            Assert.Single(fact.SecondaryLayouts);
        }
    }
}
