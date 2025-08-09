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
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Connectors
{
    internal class CatalogSourceFactory : IConnectorSourceFactory
    {
        private readonly string _catalogName;
        private readonly IConnectorSourceFactory _inner;

        public CatalogSourceFactory(string catalogName, IConnectorSourceFactory inner)
        {
            _catalogName = catalogName;
            _inner = inner;
        }
        public bool CanHandle(ReadRelation readRelation)
        {
            var copy = readRelation.Copy();
            copy.NamedTable.Names = readRelation.NamedTable.Names.Skip(1).ToList();
            return _inner.CanHandle(copy);
        }

        public IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            var copy = readRelation.Copy();
            copy.NamedTable.Names = readRelation.NamedTable.Names.Skip(1).ToList();
            return _inner.CreateSource(copy, functionsRegister, dataflowBlockOptions);
        }

        public Relation ModifyPlan(ReadRelation readRelation)
        {
            var copy = readRelation.Copy();
            copy.NamedTable.Names = readRelation.NamedTable.Names.Skip(1).ToList();
            var modifiedRelation = _inner.ModifyPlan(copy);
            // Reset the names to include the catalog name
            copy.NamedTable.Names = readRelation.NamedTable.Names.ToList();
            return modifiedRelation;
        }
    }
}
