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

namespace FlowtideDotNet.Connector.Kafka.Internal
{
    internal class KafkaSourceFactory : RegexConnectorSourceFactory
    {
        private readonly FlowtideKafkaSourceOptions options;

        public KafkaSourceFactory(string regexPattern, FlowtideKafkaSourceOptions options) : base(regexPattern)
        {
            this.options = options;
        }

        public override Relation ModifyPlan(ReadRelation readRelation)
        {
            int keyIndex = -1;
            for (int i = 0; i < readRelation.BaseSchema.Names.Count; i++)
            {
                if (readRelation.BaseSchema.Names[i] == "_key")
                {
                    keyIndex = i;
                    break;
                }
            }

            if (keyIndex == -1)
            {
                readRelation.BaseSchema.Names.Add("_key");

                if (readRelation.BaseSchema.Struct == null)
                {
                    readRelation.BaseSchema.Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>()
                    };
                }

                readRelation.BaseSchema.Struct.Types.Add(new AnyType() { Nullable = false });
                keyIndex = readRelation.BaseSchema.Names.Count - 1;
            }

            return new NormalizationRelation()
            {
                Input = readRelation,
                Filter = readRelation.Filter,
                KeyIndex = new List<int>() { keyIndex },
                Emit = readRelation.Emit
            };
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            return new KafkaDataSource(readRelation, options, dataflowBlockOptions);
        }
    }
}
