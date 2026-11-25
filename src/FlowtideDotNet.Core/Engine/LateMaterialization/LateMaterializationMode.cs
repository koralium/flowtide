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
namespace FlowtideDotNet.Core.Engine
{
    /// <summary>
    /// How a stream places row stores for wide columns.
    /// </summary>
    public enum LateMaterializationMode
    {
        /// <summary>
        /// No row store is placed.
        /// </summary>
        Off = 0,
        /// <summary>
        /// Row stores are placed where the cost model admits them.
        /// </summary>
        Auto = 1,
        /// <summary>
        /// Row stores are placed wherever the plan allows it, for testing.
        /// </summary>
        ForceOn = 2,
        /// <summary>
        /// The plan is analysed and left as it is.
        /// </summary>
        Placebo = 3
    }
}
