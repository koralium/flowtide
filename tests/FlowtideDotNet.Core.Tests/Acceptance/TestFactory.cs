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
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Acceptance
{
    internal class TestFactory : IReadWriteFactory
    {
        private readonly Func<ReadRelation, DataflowBlockOptions, IStreamIngressVertex> ingressFunc;
        private readonly Func<WriteRelation, ExecutionDataflowBlockOptions, IStreamEgressVertex> egressFunc;

        public TestFactory(Func<ReadRelation, DataflowBlockOptions, IStreamIngressVertex> ingressFunc, Func<WriteRelation, ExecutionDataflowBlockOptions, IStreamEgressVertex> egressFunc)
        {
            this.ingressFunc = ingressFunc;
            this.egressFunc = egressFunc;
        }

        public ReadOperatorInfo GetReadOperator(ReadRelation readRelation, DataflowBlockOptions dataflowBlockOptions)
        {
            return new ReadOperatorInfo(ingressFunc(readRelation, dataflowBlockOptions));
        }

        public IStreamEgressVertex GetWriteOperator(WriteRelation readRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions)
        {
            return egressFunc(readRelation, executionDataflowBlockOptions);
        }
    }
}
