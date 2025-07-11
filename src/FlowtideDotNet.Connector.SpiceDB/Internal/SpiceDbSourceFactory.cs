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
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.SpiceDB.Internal
{
    internal class SpiceDbSourceFactory : RegexConnectorSourceFactory
    {
        private readonly SpiceDbSourceOptions options;

        public SpiceDbSourceFactory(string regexPattern, SpiceDbSourceOptions options) : base(regexPattern)
        {
            this.options = options;
        }

        public override Relation ModifyPlan(ReadRelation readRelation)
        {
            // Make sure that all primary key columns are in the query, if not add them.
            List<int> indices = new List<int>();
            var userTypeIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("subject_type", StringComparison.OrdinalIgnoreCase));
            if (userTypeIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("subject_type");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                userTypeIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(userTypeIndex);
            var userIdIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("subject_id", StringComparison.OrdinalIgnoreCase));
            if (userIdIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("subject_id");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                userIdIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(userIdIndex);

            var userRelationIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("subject_relation", StringComparison.OrdinalIgnoreCase));
            if (userRelationIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("subject_relation");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                userRelationIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(userRelationIndex);

            var relationIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("relation", StringComparison.OrdinalIgnoreCase));
            if (relationIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("relation");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                relationIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(relationIndex);

            var objectTypeIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("resource_type", StringComparison.OrdinalIgnoreCase));
            if (objectTypeIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("resource_type");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                objectTypeIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(objectTypeIndex);
            var objectIdIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("resource_id", StringComparison.OrdinalIgnoreCase));
            if (objectIdIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("resource_id");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                objectIdIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(objectIdIndex);

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
            return new ColumnSpiceDbSource(options, readRelation, dataflowBlockOptions);
        }
    }
}
