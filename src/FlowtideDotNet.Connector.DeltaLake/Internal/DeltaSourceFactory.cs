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

using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    internal class DeltaSourceFactory : IConnectorSourceFactory
    {
        private readonly DeltaLakeOptions _options;
        private readonly DeltaLakeSqlProvider _provider;

        public DeltaSourceFactory(DeltaLakeOptions options, DeltaLakeSqlProvider provider)
        {
            this._options = options;
            this._provider = provider;
        }

        public bool CanHandle(ReadRelation readRelation)
        {
            return _provider.TryGetTableInformation(readRelation.NamedTable.Names, out _);
        }

        public IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            return new DeltaLakeSource(readRelation, _options, dataflowBlockOptions);
        }

        public Relation ModifyPlan(ReadRelation readRelation)
        {
            if (readRelation.Filter != null)
            {
                return new FilterRelation()
                {
                    Emit = readRelation.Emit,
                    Condition = readRelation.Filter,
                    Input = readRelation,
                };
            }

            return readRelation;
        }
    }
}
