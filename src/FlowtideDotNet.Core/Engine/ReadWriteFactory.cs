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
using FlowtideDotNet.Core.Exceptions;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Engine
{
    public class ReadWriteFactory : IReadWriteFactory
    {
        private List<Func<ReadRelation, IFunctionsRegister, DataflowBlockOptions, ReadOperatorInfo?>> _readFuncs = new List<Func<ReadRelation, IFunctionsRegister, DataflowBlockOptions, ReadOperatorInfo?>>();
        private List<Func<WriteRelation, ExecutionDataflowBlockOptions, IStreamEgressVertex?>> _writeFuncs = new List<Func<WriteRelation, ExecutionDataflowBlockOptions, IStreamEgressVertex?>>();

        public ReadWriteFactory AddReadResolver(Func<ReadRelation, IFunctionsRegister, DataflowBlockOptions, ReadOperatorInfo?> resolveFunction)
        {
            _readFuncs.Add(resolveFunction);
            return this;
        }

        public ReadWriteFactory AddWriteResolver(Func<WriteRelation, ExecutionDataflowBlockOptions, IStreamEgressVertex?> resolveFunction)
        {
            _writeFuncs.Add(resolveFunction);
            return this;
        }

        public ReadOperatorInfo GetReadOperator(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            foreach (var readfunc in _readFuncs)
            {
                var result = readfunc(readRelation, functionsRegister, dataflowBlockOptions);
                if (result != null)
                {
                    return result;
                }
            }
            throw new FlowtideException($"No read resolver matched the read relation for table: '{readRelation.NamedTable.DotSeperated}'.");
        }

        public IStreamEgressVertex GetWriteOperator(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions)
        {
            foreach (var writefunc in _writeFuncs)
            {
                var result = writefunc(writeRelation, executionDataflowBlockOptions);
                if (result != null)
                {
                    return result;
                }
            }
            throw new FlowtideException($"No write resolver matched the write relation for table '{writeRelation.NamedObject.DotSeperated}'.");
        }
    }
}
