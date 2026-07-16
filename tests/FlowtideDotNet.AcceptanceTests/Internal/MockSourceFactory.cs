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

using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    internal class MockSourceFactory : RegexConnectorSourceFactory
    {
        private readonly MockDatabase mockDatabase;
        private readonly bool immutable;
        private readonly TimeSpan? initialDataDelay;
        private readonly bool failInitialize;
        private readonly int? batchSize;

        /// <param name="initialDataDelay">
        /// Delays the initial data send, keeping the stream in its starting phase for the
        /// duration. Used to test streams whose startup is slower than their peers.
        /// </param>
        /// <param name="failInitialize">
        /// Makes every created source operator throw during initialization, used to test
        /// how a stream host handles a substream that cannot start.
        /// </param>
        public MockSourceFactory(string regexPattern, MockDatabase mockDatabase, bool immutable, TimeSpan? initialDataDelay = null, bool failInitialize = false, int? batchSize = null) : base(regexPattern)
        {
            this.mockDatabase = mockDatabase;
            this.immutable = immutable;
            this.initialDataDelay = initialDataDelay;
            this.failInitialize = failInitialize;
            this.batchSize = batchSize;
        }

        public override Relation ModifyPlan(ReadRelation readRelation)
        {
            if (immutable)
            {
                return readRelation;
            }
            var table = mockDatabase.GetTable(readRelation.NamedTable.DotSeperated);

            var emit = readRelation.Emit?.ToList();
            List<int> pks = new List<int>();
            foreach (var primaryKeyIndex in table.PrimaryKeyIndices)
            {
                var pkIndex = readRelation.BaseSchema.Names.IndexOf(table.Columns[primaryKeyIndex]);
                if (pkIndex >= 0)
                {
                    pks.Add(pkIndex);
                }
                else
                {
                    readRelation.BaseSchema.Names.Add(table.Columns[primaryKeyIndex]);
                    if (readRelation.BaseSchema.Struct == null)
                    {
                        readRelation.BaseSchema.Struct = new Struct()
                        {
                            Types = new List<SubstraitBaseType>()
                        };
                    }
                    readRelation.BaseSchema.Struct.Types.Add(new AnyType());
                    pks.Add(readRelation.BaseSchema.Names.Count - 1);
                }
            }

            List<int> readEmit = new List<int>();
            for (int i = 0; i < readRelation.BaseSchema.Names.Count; i++)
            {
                readEmit.Add(i);
            }
            readRelation.Emit = readEmit;

            return new NormalizationRelation()
            {
                Emit = emit,
                Filter = readRelation.Filter,
                Input = readRelation,
                KeyIndex = pks
            };
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            return new MockDataSourceOperator(readRelation, mockDatabase, dataflowBlockOptions, initialDataDelay, failInitialize, batchSize);
        }

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
        {
            return new TableLineageMetadata("mock", readRelation.NamedTable.DotSeperated, default);
        }
    }
}
