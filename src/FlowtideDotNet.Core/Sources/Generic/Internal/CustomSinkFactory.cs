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
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Sources.Generic.Internal
{
    public class CustomSinkFactory<T> : AbstractConnectorSinkFactory
    {
        private readonly string tableName;
        private readonly Func<WriteRelation, GenericDataSink<T>> dataSinkFunc;
        private readonly ExecutionMode executionMode;

        public CustomSinkFactory(string tableName, Func<WriteRelation, GenericDataSink<T>> dataSinkFunc, ExecutionMode executionMode)
        {
            this.tableName = tableName;
            this.dataSinkFunc = dataSinkFunc;
            this.executionMode = executionMode;
        }

        public override bool CanHandle(WriteRelation writeRelation)
        {
            return writeRelation.NamedObject.DotSeperated.Equals(tableName, StringComparison.OrdinalIgnoreCase);
        }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
        {
            if (writeRelation.Overwrite)
            {
                throw new NotSupportedException("Custom sink sink does not support overwrite.");
            }

            return new GenericWriteOperator<T>(dataSinkFunc(writeRelation), executionMode, writeRelation, dataflowBlockOptions);
        }
    }
}
