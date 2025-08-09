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

using FlowtideDotNet.Base.Vertices.Egress;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    internal class DeltaLakeSinkFactory : IConnectorSinkFactory
    {
        private readonly DeltaLakeOptions options;

        public DeltaLakeSinkFactory(DeltaLakeOptions options)
        {
            this.options = options;
        }

        public bool CanHandle(WriteRelation writeRelation)
        {
            return true;
        }

        public IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
        {
            return new DeltaLakeSink(options, writeRelation, dataflowBlockOptions);
        }

        public Relation ModifyPlan(WriteRelation writeRelation)
        {
            return writeRelation;
        }
    }
}
