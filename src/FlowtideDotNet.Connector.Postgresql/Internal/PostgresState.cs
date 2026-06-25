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
    /// Persisted operator state. With temporary slots the source re-snapshots on restart, so the LSN is only kept for
    /// observability; correctness comes from the reconciling full load in the base read operator.
    /// </summary>
    internal class PostgresState
    {
        public long LastLsn { get; set; }
    }
}
