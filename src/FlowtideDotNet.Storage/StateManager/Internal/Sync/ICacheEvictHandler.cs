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

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    /// <summary>
    /// Implemented by state clients so the shared cache table can hand back a batch of
    /// eviction candidates for serialization to temporary storage before they are removed
    /// from memory. Each tuple carries the entry and the entry version at selection time;
    /// the table re-checks the version before removal so values modified during
    /// serialization stay cached.
    /// </summary>
    internal interface ICacheEvictHandler
    {
        void Evict(List<(S3FifoCacheEntry, long)> valuesToEvict, bool isCleanup);
    }
}
