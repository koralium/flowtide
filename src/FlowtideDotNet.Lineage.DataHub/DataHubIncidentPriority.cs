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

namespace FlowtideDotNet.Lineage.DataHub
{
    /// <summary>
    /// Priority of a DataHub incident.
    /// </summary>
    public enum DataHubIncidentPriority
    {
        /// <summary>
        /// Critical.
        /// </summary>
        Critical = 0,

        /// <summary>
        /// High.
        /// </summary>
        High = 1,

        /// <summary>
        /// Medium.
        /// </summary>
        Medium = 2,

        /// <summary>
        /// Low.
        /// </summary>
        Low = 3
    }
}
