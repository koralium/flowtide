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
    internal class StateClientMetadata<T>
    {
        public T? Metadata { get; set; }

        /// <summary>
        /// Used to track if the state has been commited at least once, if it is true, it is known that data can be fetched from persitent storage.
        /// A temporary tree will always have this set to false.
        /// </summary>
        public bool CommitedOnce { get; set; }
    }
}
