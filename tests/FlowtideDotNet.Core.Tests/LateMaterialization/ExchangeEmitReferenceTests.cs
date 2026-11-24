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
using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class ExchangeEmitReferenceTests
    {
        private static List<Relation> Plan(bool broadcast, bool exchangeEmit)
        {
            var exchange = new ExchangeRelation
            {
                Input = new ReadRelation
                {
                    NamedTable = new NamedTable { Names = ["t"] },
                    BaseSchema = new NamedStruct { Names = ["key", "payload"] },
                    RowStore = new RowStoreDefinition { Name = "rows", StoredColumns = [1] }
                },
                ExchangeKind = broadcast
                    ? new BroadcastExchangeKind()
                    : new ScatterExchangeKind
                    {
                        Fields = [new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = 0 } }]
                    },
                PartitionCount = 1,
                Targets = [new StandardOutputExchangeTarget { PartitionIds = [0] }]
            };
            var output = new StandardOutputExchangeReferenceRelation
            {
                RelationId = 0,
                TargetId = 0,
                ReferenceOutputLength = 2
            };
            if (exchangeEmit)
            {
                exchange.Emit = [1, 0];
            }
            else
            {
                output.Emit = [1, 0];
            }
            var fetch = new FetchRequiredColumnsRelation
            {
                Input = output,
                Fetches = [new RowStoreFetch { StoreName = "rows", ReferenceColumn = 0, Columns = [0] }],
                Emit = [1, 2]
            };
            return
            [
                exchange,
                new WriteRelation
                {
                    Input = fetch,
                    NamedObject = new NamedTable { Names = ["out"] },
                    TableSchema = new NamedStruct { Names = ["key", "payload"] }
                }
            ];
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void AnExchangeEmitThatMovesAReferenceMustBeRefused(bool broadcast)
        {
            Assert.Throws<InvalidOperationException>(() => RowReferencePlanValidator.Validate(Plan(broadcast, true)));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void AnExchangeOutputEmitThatMovesAReferenceMustBeRefused(bool broadcast)
        {
            Assert.Throws<InvalidOperationException>(() => RowReferencePlanValidator.Validate(Plan(broadcast, false)));
        }

        private static DirectFieldReference Field(int field)
        {
            return new DirectFieldReference { ReferenceSegment = new StructReferenceSegment { Field = field } };
        }

        private static Plan WrongStorePlan(bool hosted, bool exchangeEmit)
        {
            ReadRelation Read(string table)
            {
                return new ReadRelation
                {
                    NamedTable = new NamedTable { Names = [table] },
                    BaseSchema = new NamedStruct
                    {
                        Names = ["k", "n", "text", "note"],
                        Struct = new Struct { Types = [new AnyType(), new AnyType(), new AnyType(), new AnyType()] }
                    },
                    RowStore = hosted ? new RowStoreDefinition { Name = table, StoredColumns = [2, 3] } : null
                };
            }
            var left = Read("l");
            var join = new MergeJoinRelation
            {
                Left = left,
                Right = Read("r"),
                Type = JoinType.Inner,
                LeftKeys = [Field(1)],
                RightKeys = [Field(left.OutputLength)]
            };
            var relations = new List<Relation>();
            Relation input = join;
            if (hosted)
            {
                var exchange = new ExchangeRelation
                {
                    Input = join,
                    ExchangeKind = new ScatterExchangeKind { Fields = [Field(0)] },
                    PartitionCount = 1,
                    Targets = [new StandardOutputExchangeTarget { PartitionIds = [0] }]
                };
                var output = new StandardOutputExchangeReferenceRelation
                {
                    RelationId = 0,
                    TargetId = 0,
                    ReferenceOutputLength = 6
                };
                if (exchangeEmit)
                {
                    exchange.Emit = [0, 1, 5, 3, 4, 2];
                }
                else
                {
                    output.Emit = [0, 1, 5, 3, 4, 2];
                }
                relations.Add(exchange);
                input = new FetchRequiredColumnsRelation
                {
                    Input = output,
                    Fetches =
                    [
                        new RowStoreFetch { StoreName = "r", ReferenceColumn = 2, Columns = [0, 1] },
                        new RowStoreFetch { StoreName = "l", ReferenceColumn = 5, Columns = [0, 1] }
                    ],
                    Emit = [0, 1, 8, 9, 3, 4, 6, 7]
                };
            }
            relations.Add(new WriteRelation
            {
                Input = input,
                NamedObject = new NamedTable { Names = ["out"] },
                TableSchema = new NamedStruct { Names = Enumerable.Range(0, 8).Select(i => "c" + i).ToList() }
            });
            return new Plan { Relations = relations };
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AnEmitThatSwapsReferenceColumnsIsRefusedAtBuild(bool exchangeEmit)
        {
            DifferentialTable[] Tables()
            {
                var left = new DifferentialTable("l", 4);
                left.Load([1L, 20L, "A1", "a"], [2L, 10L, "A2", "b"]);
                var right = new DifferentialTable("r", 4);
                right.Load([10L, 0L, "B10", "c"], [20L, 0L, "B20", "d"]);
                return [left, right];
            }
            await using var pair = new DifferentialPair(hosted => WrongStorePlan(hosted, exchangeEmit), Tables);

            var error = await Assert.ThrowsAsync<InvalidOperationException>(pair.Hosted.StartAsync);

            Assert.Contains("the emit is not applied when the stream runs", error.Message);
        }
    }
}
