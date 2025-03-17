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
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.SpiceDB.Internal
{
    internal class SpiceDbSinkFactory : RegexConnectorSinkFactory
    {
        private readonly SpiceDbSinkOptions spiceDbSinkOptions;

        public SpiceDbSinkFactory(string regexPattern, SpiceDbSinkOptions spiceDbSinkOptions) : base(regexPattern)
        {
            this.spiceDbSinkOptions = spiceDbSinkOptions;
        }

        /// <summary>
        /// Modify the plan to add cast to string infront of each column to make sure all data are strings
        /// </summary>
        /// <param name="writeRelation"></param>
        /// <returns></returns>
        public override Relation ModifyPlan(WriteRelation writeRelation)
        {
            List<Expression> expressions = new List<Expression>();
            List<int> emit = new List<int>();
            var emitStart = writeRelation.Input.OutputLength;
            for (int i = 0; i < writeRelation.TableSchema.Names.Count; i++)
            {
                emit.Add(emitStart + i);
                expressions.Add(new CastExpression()
                {
                    Expression = new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment()
                        {
                            Field = i
                        }
                    },
                    Type = new StringType()
                });
            }

            writeRelation.Input = new ProjectRelation()
            {
                Emit = emit,
                Input = writeRelation.Input,
                Expressions = expressions
            };
            return writeRelation;
        }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
        {
            return new ColumnSpiceDbSink(spiceDbSinkOptions, spiceDbSinkOptions.ExecutionMode, writeRelation, dataflowBlockOptions);
        }
    }
}
