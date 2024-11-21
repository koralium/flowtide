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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.MongoDB
{
    public class FlowtideMongoDbSourceOptions
    {
        public required string ConnectionString { get; set; }

        /// <summary>
        /// Allows flowtide to do a full reload of the data on interval when watch/change stream is not supported.
        /// </summary>
        public bool EnableFullReloadForNonReplicaSets { get; set; } = true;

        /// <summary>
        /// Interval for full reload of the data when watch/change stream is not supported.
        /// </summary>
        public TimeSpan FullReloadIntervalForNonReplicaSets { get; set; } = TimeSpan.FromMinutes(10);

        /// <summary>
        /// Used only for testing, to see that the connector functions when operation time is not available.
        /// </summary>
        internal bool DisableOperationTime { get; set; }
    }
}
