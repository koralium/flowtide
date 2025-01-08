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

using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Storage.StateManager.Internal
{
    public interface ICacheObject : ILockableObject
    {
        /// <summary>
        /// Try and rent the object, returns true if successful.
        /// If false, the object is no longer in the cache.
        /// </summary>
        /// <returns></returns>
        bool TryRent();

        void Return();

        int RentCount { get; }

        bool RemovedFromCache { get; set; }
    }
}
