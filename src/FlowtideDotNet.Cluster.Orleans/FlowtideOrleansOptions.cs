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

namespace FlowtideDotNet.Cluster.Orleans
{
    /// <summary>
    /// Options for streams hosted in Orleans.
    /// </summary>
    public class FlowtideOrleansOptions
    {
        /// <summary>
        /// Called with the stream name, substream name and the builder of each substream
        /// before it is built, after the substream grain has applied its own configuration.
        /// Can be used to change stream options such as the stop drain timeout, per stream
        /// or per substream.
        /// </summary>
        public Action<string, string, FlowtideBuilder>? ConfigureBuilder { get; set; }
    }
}
