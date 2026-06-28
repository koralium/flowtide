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

namespace FlowtideDotNet.Connector.PostgreSQL.Internal
{
    /// <summary>
    /// Persisted operator state. <see cref="LastLsn"/> is the last WAL position applied by a globally-committed
    /// checkpoint. With a temporary slot the source re-snapshots on restart, so it is mainly informational there;
    /// with a persistent slot it is load-bearing — it is the position the slot resumes from (and is confirmed to the
    /// server one checkpoint behind), so it must never run ahead of durably-applied data.
    /// </summary>
    internal class PostgresState
    {
        public long LastLsn { get; set; }
    }
}
