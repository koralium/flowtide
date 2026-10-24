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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake
{
    /// <summary>
    /// Requests for the background maintenance of Delta Lake tables that sinks in this process write.
    /// </summary>
    public static class DeltaLakeMaintenance
    {
        /// <summary>
        /// Asks the running sink of a table, on the storage and with the table name it was given, to sweep orphan files once; never throws.
        /// </summary>
        public static OrphanSweepRequestResult RequestOrphanSweep(IFileStorage storage, string tablePath)
        {
            return RequestOrphanSweep(storage, tablePath, DeltaMaintenanceBudget.SharedSource);
        }

        internal static OrphanSweepRequestResult RequestOrphanSweep(IFileStorage storage, string tablePath, DeltaMaintenanceBudget.SharedBudget source)
        {
            try
            {
                // Never creates the budget
                if (source.Existing is not { } budget)
                {
                    return OrphanSweepRequestResult.Unavailable;
                }
                // Formed as the sink forms its route
                var route = DiscardRoute.Of(storage, new IOPath(tablePath));
                return budget.WorkerFor(route)?.Cleanup is DeltaTableCleanup cleanup ? cleanup.RequestOrphanSweep() : OrphanSweepRequestResult.Unavailable;
            }
            catch
            {
                return OrphanSweepRequestResult.Unavailable;
            }
        }
    }
}
