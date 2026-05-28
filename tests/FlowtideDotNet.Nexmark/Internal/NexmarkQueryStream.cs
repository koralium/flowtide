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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Benchmarks.Stream;
using FlowtideDotNet.Core;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using System;
using System.Collections.Generic;
using System.Text;

namespace FlowtideDotNet.Nexmark.Internal
{
    public class NexmarkQueryStream : FlowtideTestStream
    {
        private int checkpointCounter = 0;

        private readonly string _baseDir;

        public NexmarkQueryStream(string baseDir, string testName) : base(testName)
        {
            _baseDir = baseDir;
        }

        protected override void AddReadResolvers(IConnectorManager connectorManger)
        {
            connectorManger.AddSource(new NexmarkSourceFactory(".*", _baseDir));
        }

        protected override void AddWriteResolvers(IConnectorManager connectorManger)
        {
            connectorManger.AddSink(new BenchmarkWriteOperatorFactory("*", () =>
            {
                checkpointCounter++;
            }));
        }

        public override async Task WaitForUpdate()
        {
            int currentCounter = checkpointCounter;
            while (checkpointCounter == currentCounter)
            {
                await SchedulerTick();
                await Task.Delay(10);
            }
        }

        protected override IPersistentStorage CreatePersistentStorage(string testName, bool ignoreSameDataCheck)
        {
            return new ReservoirPersistentStorage(new ReservoirStorageOptions()
            {
                FileProvider = new MemoryFileProvider()
            });
        }
    }
}
