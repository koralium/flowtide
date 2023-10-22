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

using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.StateManager.Internal.Simple;

namespace FlowtideDotNet.Storage.StateManager.WorkerThread
{
    public class StateManagerWithWorker : IStateManager
    {
        private LRUTableSimple lruTable;
        private Functions m_functions;

        public bool Initialized => throw new NotImplementedException();

        public ValueTask CheckpointAsync()
        {
            throw new NotImplementedException();
        }

        public Task Compact()
        {
            throw new NotImplementedException();
        }

        public IStateManagerClient GetOrCreateClient(string name)
        {
            throw new NotImplementedException();
        }

        public Task InitializeAsync()
        {
            throw new NotImplementedException();
        }
    }
}
