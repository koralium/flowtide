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

using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Text.Json;

namespace FlowtideDotNet.Base
{
    public class Checkpoint : ICheckpointEvent
    {
        //private readonly ConcurrentDictionary<string, JsonElement> _operatorStates = new ConcurrentDictionary<string, JsonElement>();

        public long CheckpointTime { get; }

        public long NewTime { get; }

        //public void AddState<T>(string name, T state)
        //{
        //    var serialized = JsonSerializer.SerializeToElement(state);
        //    _operatorStates.AddOrUpdate(name, serialized, (k, o) => serialized);
        //}

        //internal ImmutableDictionary<string, JsonElement> GetOperatorStates()
        //{
        //    return _operatorStates.ToImmutableDictionary();
        //}

        public Checkpoint(long checkpointTime, long newTime)
        {
            CheckpointTime = checkpointTime;
            NewTime = newTime;
        }
    }
}
