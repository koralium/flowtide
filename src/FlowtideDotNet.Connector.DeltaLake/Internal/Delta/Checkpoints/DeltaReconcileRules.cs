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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// The one set of reconciliation rules that log replay, the tail reducer and the sort reducer apply.
    /// </summary>
    internal static class DeltaReconcileRules
    {
        // Adds and removes share the file family; commitInfo and cdc are never reconciled
        public static RecordFamily Family(RecordKind kind) => CheckpointRecord.Family(kind);

        // A file is keyed by its path and its deletion vector's unique id
        public static DeltaFileKey FileKey(string path, DeletionVector? deletionVector) => new DeltaFileKey(path, deletionVector?.UniqueId);

        // A later reference to a key replaces an earlier one; a txn's own version is not compared, as it need not grow
        public static bool LaterReplaces(RecordFamily family, long? earlierTxnVersion, long? laterTxnVersion) => true;

        // A removed domain suppresses the older ones and is never written itself
        public static bool IsWritten(DeltaDomainMetadataAction domain) => !domain.Removed;
    }
}
