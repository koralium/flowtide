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

using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Lineage.DataHub.Internal;

namespace FlowtideDotNet.Lineage.DataHub
{
    public static class FlowtideBuilderDataHubExtensions
    {
        /// <summary>
        /// Registers the stream lineage in a DataHub lineage store.
        /// </summary>
        /// <param name="builder">The stream builder to opt in.</param>
        /// <param name="store">Store that serves the DataHub entities.</param>
        /// <returns>The same builder for chaining.</returns>
        public static FlowtideBuilder WithDataHubLineageStore(this FlowtideBuilder builder, DataHubLineageStore store)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(store);
            return builder.AddLineageListener(new DataHubLineageListener(store));
        }
    }
}
