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
using FlowtideDotNet.Substrait.Expressions;
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

            if (keyIndex == -1 && !options.IsImmutable)
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

            // The source ignores filter and emit, the wrapper applies them
            var filter = readRelation.Filter;
            var emit = readRelation.Emit;
            readRelation.Filter = null;
            readRelation.Emit = null;

            if (options.IsImmutable)
            {
                if (filter != null)
                {
                    return new FilterRelation()
                    {
                        Condition = filter,
                        Input = readRelation,
                        Emit = emit
                    };
                }
                // An emit that keeps every column in place needs no operator
                if (emit != null && !IsIdentity(emit, readRelation.BaseSchema.Names.Count))
                {
                    return new ProjectRelation()
                    {
                        Expressions = new List<Expression>(),
                        Input = readRelation,
                        Emit = emit
                    };
                }
                return readRelation;
            }

            return new NormalizationRelation()
            {
                Input = readRelation,
                Filter = filter,
                KeyIndex = new List<int>() { keyIndex },
                Emit = emit
            };
        }

        private static bool IsIdentity(List<int> emit, int columns)
        {
            if (emit.Count != columns)
            {
                return false;
            }
            for (int i = 0; i < emit.Count; i++)
            {
                if (emit[i] != i)
                {
                    return false;
                }
            }
            return true;
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            return new KafkaDataSource(readRelation, options, dataflowBlockOptions);
        }

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
        {
            if (options.ConsumerConfig != null)
            {
                return new TableLineageMetadata($"kafka://{options.ConsumerConfig.BootstrapServers}", readRelation.NamedTable.DotSeperated, default);
            }
            return new TableLineageMetadata("kafka", readRelation.NamedTable.DotSeperated, default);
        }
    }
}
