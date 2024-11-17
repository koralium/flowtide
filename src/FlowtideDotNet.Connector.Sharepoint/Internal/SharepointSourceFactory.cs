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
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using Substrait.Protobuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.Sharepoint.Internal
{
    internal class SharepointSourceFactory : AbstractConnectorSourceFactory, IConnectorTableProviderFactory
    {
        private readonly SharepointSourceOptions sharepointSourceOptions;
        private readonly SharepointTableProvider tableProvider;

        public SharepointSourceFactory(SharepointSourceOptions sharepointSourceOptions, string prefix = "")
        {
            this.sharepointSourceOptions = sharepointSourceOptions;
            tableProvider = new SharepointTableProvider(sharepointSourceOptions, prefix);
        }

        public override bool CanHandle(ReadRelation readRelation)
        {
            return tableProvider.TryGetTableInformation(readRelation.NamedTable.DotSeperated, out _);
        }

        public ITableProvider Create()
        {
            return tableProvider;
        }

        public override Relation ModifyPlan(ReadRelation readRelation)
        {
            List<int> indices = new List<int>();

            var idIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("Id", StringComparison.OrdinalIgnoreCase));
            if (idIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("Id");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                idIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(idIndex);

            return new NormalizationRelation()
            {
                Input = readRelation,
                Filter = readRelation.Filter,
                KeyIndex = indices,
                Emit = readRelation.Emit
            };
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            var listId = tableProvider.GetListId(readRelation.NamedTable.DotSeperated);
            return new SharepointSource(sharepointSourceOptions, listId, readRelation, dataflowBlockOptions);
        }
    }
}
