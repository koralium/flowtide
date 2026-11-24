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

using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Core.Sinks;
using FlowtideDotNet.Core.Tests.Failure;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class RowReferenceEmitValidatorTests
    {
        private static Plan PlanWithReferenceEmit()
        {
            var host = new NormalizationRelation()
            {
                Input = new ReadRelation()
                {
                    NamedTable = new NamedTable() { Names = ["t"] },
                    BaseSchema = new NamedStruct()
                    {
                        Names = ["k", "payload"],
                        Struct = new Struct() { Types = [new AnyType(), new AnyType()] }
                    }
                },
                KeyIndex = [0],
                RowStore = new RowStoreDefinition() { Name = "rows", StoredColumns = [1] }
            };
            return new Plan()
            {
                Relations =
                [
                    host,
                    new WriteRelation()
                    {
                        Input = new ReferenceRelation()
                        {
                            RelationId = 0,
                            ReferenceOutputLength = 2,
                            Emit = [0]
                        },
                        NamedObject = new NamedTable() { Names = ["output"] },
                        TableSchema = new NamedStruct() { Names = ["k", "payload"] }
                    }
                ]
            };
        }

        [Fact]
        public void ReferenceEmitCannotHideAReferenceFromTheValidator()
        {
            var plan = PlanWithReferenceEmit();

            Assert.Throws<InvalidOperationException>(() => RowReferencePlanValidator.Validate(plan.Relations));
        }

        [Fact]
        public async Task ReferenceEmitCannotHideAReferenceAtBuild()
        {
            var connectors = new ConnectorManager();
            connectors.AddSource(new FailureIngressFactory("*"));
            connectors.AddConsoleSink(".*");
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var builder = new FlowtideBuilder("reference-emit-validation")
                .AddPlan(PlanWithReferenceEmit(), optimize: false)
                .AddConnectorManager(connectors)
                .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage });
            Base.Engine.DataflowStream? stream = null;
            try
            {
                var exception = Record.Exception(() => stream = builder.Build());

                Assert.IsType<InvalidOperationException>(exception);
            }
            finally
            {
                if (stream != null)
                {
                    await stream.DisposeAsync();
                }
            }
        }
    }
}
