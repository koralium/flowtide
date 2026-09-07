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
    /// Lets the cache table hand a state client a batch of victims to serialize before removal.
    /// Each tuple carries the entry and its version at selection time, which the table
    /// re-checks so values modified during serialization stay cached.
    /// Returns false when the handler declines the batch, such as while its commit is in
    /// flight; the table keeps those victims cached and retries them on a later pass.
    /// </summary>
    internal interface ICacheEvictHandler
    {
        bool Evict(List<(S3FifoCacheEntry, long)> valuesToEvict, bool isCleanup);
    }
}
