﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using FlowtideDotNet.Core.Exceptions;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Engine
{
    public class ReadWriteFactory : IReadWriteFactory
    {
        private List<Func<ReadRelation, DataflowBlockOptions, ReadOperatorInfo?>> _readFuncs = new List<Func<ReadRelation, DataflowBlockOptions, ReadOperatorInfo?>>();
        private List<Func<WriteRelation, ExecutionDataflowBlockOptions, IStreamEgressVertex?>> _writeFuncs = new List<Func<WriteRelation, ExecutionDataflowBlockOptions, IStreamEgressVertex?>>();

        public ReadWriteFactory AddReadResolver(Func<ReadRelation, DataflowBlockOptions, ReadOperatorInfo?> resolveFunction)
        {
            _readFuncs.Add(resolveFunction);
            return this;
        }

        public ReadWriteFactory AddWriteResolver(Func<WriteRelation, ExecutionDataflowBlockOptions, IStreamEgressVertex?> resolveFunction)
        {
            _writeFuncs.Add(resolveFunction);
            return this;
        }

        public ReadOperatorInfo GetReadOperator(ReadRelation readRelation, DataflowBlockOptions dataflowBlockOptions)
        {
            foreach(var readfunc in _readFuncs)
            {
                var result = readfunc(readRelation, dataflowBlockOptions);
                if (result != null) 
                {
                    return result;
                }
            }
            throw new FlowtideException("No read resolver matched the read relation.");
        }

        public IStreamEgressVertex GetWriteOperator(WriteRelation readRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions)
        {
            foreach(var writefunc in _writeFuncs)
            {
                var result = writefunc(readRelation, executionDataflowBlockOptions);
                if (result != null)
                {
                    return result;
                }
            }
            throw new FlowtideException("No write resolver matched the read relation.");
        }
    }
}
