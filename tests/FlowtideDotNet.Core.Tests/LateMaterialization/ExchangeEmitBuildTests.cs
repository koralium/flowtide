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
    public class ExchangeEmitBuildTests
    {
        // k, n, ref when hosted; text and note stored
        private static ReadRelation Read(bool hosted)
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = ["t"] },
                BaseSchema = new NamedStruct()
                {
                    Names = ["k", "n", "text", "note"],
                    Struct = new Struct() { Types = [new AnyType(), new AnyType(), new AnyType(), new AnyType()] }
                },
                RowStore = hosted ? new RowStoreDefinition() { Name = "t", StoredColumns = [2, 3] } : null
            };
        }

        private static ExchangeRelation Broadcast(Relation input)
        {
            return new ExchangeRelation()
            {
                Input = input,
                ExchangeKind = new BroadcastExchangeKind(),
                Targets = [new StandardOutputExchangeTarget() { PartitionIds = [0] }]
            };
        }

        private static WriteRelation Write(Relation input, int columns)
        {
            return new WriteRelation()
            {
                Input = input,
                NamedObject = new NamedTable() { Names = ["output"] },
                TableSchema = new NamedStruct() { Names = Enumerable.Range(0, columns).Select(x => "c" + x).ToList() }
            };
        }

        // The exchange's Emit drops the reference, the runtime ignores that Emit
        private static Plan ExchangeEmitDropsTheReference()
        {
            var exchange = Broadcast(Read(true));
            exchange.Emit = [0, 1];
            return new Plan()
            {
                Relations =
                [
                    exchange,
                    Write(new StandardOutputExchangeReferenceRelation() { RelationId = 0, TargetId = 0, ReferenceOutputLength = 2 }, 2)
                ]
            };
        }

        // The output's Emit drops the reference, the runtime ignores it
        private static Plan ExchangeOutputEmitDropsTheReference()
        {
            return new Plan()
            {
                Relations =
                [
                    Broadcast(Read(true)),
                    Write(new StandardOutputExchangeReferenceRelation() { RelationId = 0, TargetId = 0, ReferenceOutputLength = 3, Emit = [0, 1] }, 2)
                ]
            };
        }

        [Fact]
        public void AnExchangeEmitCannotHideAReferenceFromTheValidator()
        {
            Assert.Throws<InvalidOperationException>(() => RowReferencePlanValidator.Validate(ExchangeEmitDropsTheReference().Relations));
        }

        [Fact]
        public void AnExchangeOutputEmitCannotHideAReferenceFromTheValidator()
        {
            Assert.Throws<InvalidOperationException>(() => RowReferencePlanValidator.Validate(ExchangeOutputEmitDropsTheReference().Relations));
        }

        // What the sink got, or the build error
        private static async Task<string> RunAsync(Func<Plan> plan)
        {
            var table = new DifferentialTable("t", 4);
            table.Load([1L, 5L, "a", "x"], [2L, 20L, "b", null]);
            await using var stream = new DifferentialStream(plan, null, table);
            try
            {
                await stream.StartAsync();
            }
            catch (InvalidOperationException e)
            {
                return "refused: " + e.Message;
            }
            var until = DateTime.UtcNow.AddSeconds(30);
            while (DateTime.UtcNow < until && stream.Result().Count < 2)
            {
                await Task.Delay(25);
            }
            return "sink got: " + string.Join(", ", stream.Result());
        }

        [Fact]
        public async Task AnExchangeEmitThatTheRuntimeIgnoresIsRefusedAtBuild()
        {
            var outcome = await RunAsync(ExchangeEmitDropsTheReference);
            Assert.StartsWith("refused: ", outcome);
        }

        [Fact]
        public async Task AnExchangeOutputEmitThatTheRuntimeIgnoresIsRefusedAtBuild()
        {
            var outcome = await RunAsync(ExchangeOutputEmitDropsTheReference);
            Assert.StartsWith("refused: ", outcome);
        }
    }
}
