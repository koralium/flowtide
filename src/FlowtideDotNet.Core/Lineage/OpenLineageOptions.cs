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

namespace FlowtideDotNet.Core.Lineage
{
    /// <summary>
    /// Options shared by all OpenLineage transports
    /// </summary>
    public class OpenLineageOptions
    {
        /// <summary>
        /// Gets or sets a value indicating whether schema information should be included in the lineage events.
        /// </summary>
        public bool IncludeSchema { get; set; }

        /// <summary>
        /// Gets or sets an optional run identifier for the lineage events.
        /// </summary>
        /// <remarks>
        /// When set, this <see cref="Guid"/> is used as the run ID for all lineage events emitted by the reporter.
        /// If <see langword="null"/>, a new <see cref="Guid"/> is generated automatically.
        /// </remarks>
        public Guid? RunId { get; set; }
    }
}
