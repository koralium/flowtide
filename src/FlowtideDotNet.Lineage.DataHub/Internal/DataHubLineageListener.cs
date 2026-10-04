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

using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Core.Lineage.Internal.Models;

namespace FlowtideDotNet.Lineage.DataHub.Internal
{
    internal sealed class DataHubLineageListener : IStreamLineageListener
    {
        private readonly DataHubLineageStore _store;

        public DataHubLineageListener(DataHubLineageStore store)
        {
            _store = store;
        }

        public bool IncludeConnectorSchema => _store.IncludeConnectorSchema;

        public void OnStreamBuilt(StreamLineage lineage, string logicalStreamName)
        {
            _store.Register(lineage, logicalStreamName);
        }
    }
}
