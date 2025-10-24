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

using FlowtideDotNet.Connector.StarRocks.Internal;
using FlowtideDotNet.Core.Operators.Write;

namespace FlowtideDotNet.Connector.StarRocks
{
    public class StarRocksSinkOptions
    {
        public required string HttpUrl { get; set; }

        /// <summary>
        /// Optional backend http url mostly used for unit testing with the allin1 docker iamge.
        /// This allows insertions even when using random ports.
        /// </summary>
        public string? BackendHttpUrl { get; set; }

        public required Func<StarRocksUsernamePassword> Credentials { get; set; }


        public int BatchSize { get; set; } = 1000;

        /// <summary>
        /// When the connector should commit data to starrocks
        /// On checkpoint allows exactly-once semantics but may have higher latency
        /// 
        /// Default: OnCheckpoint
        /// </summary>
        public ExecutionMode ExecutionMode { get; set; } = ExecutionMode.OnCheckpoint;

        internal IStarRocksClientFactory ClientFactory { get; set; } = new DefaultStarRocksClientFactory();
    }
}
