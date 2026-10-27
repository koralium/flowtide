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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Deletes
{
    // Internal knobs for the sink's delete and intake paths
    internal sealed class DeleteLimits
    {
        public static readonly DeleteLimits Default = new DeleteLimits();

        // Input batches go through the bulk inserter
        public bool BulkIntake { get; set; } = true;
    }
}
