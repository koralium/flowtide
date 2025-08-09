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
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Connectors
{
    internal class CatalogSinkFactory : IConnectorSinkFactory
    {
        private readonly string _catalogName;
        private readonly IConnectorSinkFactory _inner;

        public CatalogSinkFactory(string catalogName, IConnectorSinkFactory inner)
        {
            _catalogName = catalogName;
            _inner = inner;
        }

        public bool CanHandle(WriteRelation writeRelation)
        {
            var copy = writeRelation.ShallowCopy();
            copy.NamedObject.Names = writeRelation.NamedObject.Names.Skip(1).ToList();
            return _inner.CanHandle(copy);
        }

        public IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
        {
            var copy = writeRelation.ShallowCopy();
            copy.NamedObject.Names = writeRelation.NamedObject.Names.Skip(1).ToList();
            return _inner.CreateSink(copy, functionsRegister, dataflowBlockOptions);
        }

        public Relation ModifyPlan(WriteRelation writeRelation)
        {
            var copy = writeRelation.ShallowCopy();
            copy.NamedObject.Names = writeRelation.NamedObject.Names.Skip(1).ToList();
            var modifiedRelation = _inner.ModifyPlan(copy);
            // Reset the names to include the catalog name
            copy.NamedObject.Names = writeRelation.NamedObject.Names.ToList();
            return modifiedRelation;
        }
    }
}
