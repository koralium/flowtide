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

namespace FlowtideDotNet.Storage.StateManager.Internal
{
    internal class StateManagerMetadata<T> : StateManagerMetadata
    {
        public T? Metadata { get; set; }
    }

    internal class StateManagerMetadata : ICacheObject
    {
        public StateManagerMetadata()
        {
            //Checkpoints = new List<long>();
            PageCounter = 2;
            ClientMetadataLocations = new Dictionary<string, long>();
        }

        /// <summary>
        /// Contains a list of checkpoints
        /// </summary>
        //public List<long> Checkpoints { get; set; }

        public long CheckpointVersion { get; set; }

        public long PageCounter { get; set; }

        public Dictionary<string, long> ClientMetadataLocations { get; set; }

        public void EnterWriteLock()
        {
            throw new NotImplementedException();
        }

        public void ExitWriteLock()
        {
            throw new NotImplementedException();
        }
    }
}
