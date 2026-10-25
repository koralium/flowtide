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

using Microsoft.Extensions.Logging;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta
{
    internal sealed class DeltaReadOptions
    {
        public static readonly DeltaReadOptions Default = new DeltaReadOptions();

        // Removes still drop their adds, the tombstones are just not kept
        public bool SkipTombstones { get; init; }

        public ILogger? Logger { get; init; }

        // Checkpoint failures already logged as a warning, repeats are logged at debug level
        public ISet<string>? ReportedCheckpointFailures { get; init; }
    }
}
