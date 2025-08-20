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

using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.FasterStorage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    internal class PersistentFlowtideTestStream : FlowtideTestStream
    {
        public PersistentFlowtideTestStream(string testName) : base(testName)
        {
        }

        protected override IPersistentStorage CreatePersistentStorage(string testName, bool ignoreSameDataCheck)
        {
            if (Directory.Exists($"./data/tempFiles/faster/{testName}/persist"))
            {
                Directory.Delete($"./data/tempFiles/faster/{testName}/persist", true);
            }
            return new FasterKvPersistentStorage(new FASTER.core.FasterKVSettings<long, FASTER.core.SpanByte>($"./data/tempFiles/faster/{testName}/persist")
            {
                RemoveOutdatedCheckpoints = false
            });
        }
    }
}
