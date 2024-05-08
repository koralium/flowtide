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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.OpenFGA.Internal
{
    internal class OpenFgaSourceFactory : RegexConnectorSourceFactory
    {
        private readonly OpenFgaSourceOptions options;

        public OpenFgaSourceFactory(string regexPattern, OpenFgaSourceOptions options) : base(regexPattern)
        {
            this.options = options;
        }

        public override Relation ModifyPlan(ReadRelation readRelation)
        {
            // Make sure that all primary key columns are in the query, if not add them.
            List<int> indices = new List<int>();
            var userTypeIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("user_type", StringComparison.OrdinalIgnoreCase));
            if (userTypeIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("user_type");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                userTypeIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(userTypeIndex);
            var userIdIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("user_id", StringComparison.OrdinalIgnoreCase));
            if (userIdIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("user_id");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                userIdIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(userIdIndex);

            var userRelationIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("user_relation", StringComparison.OrdinalIgnoreCase));
            if (userRelationIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("user_relation");
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

            var objectTypeIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("object_type", StringComparison.OrdinalIgnoreCase));
            if (objectTypeIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("object_type");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                objectTypeIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(objectTypeIndex);
            var objectIdIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("object_id", StringComparison.OrdinalIgnoreCase));
            if (objectIdIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("object_id");
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
            return new FlowtideOpenFgaSource(options, readRelation, dataflowBlockOptions);
        }
    }
}
