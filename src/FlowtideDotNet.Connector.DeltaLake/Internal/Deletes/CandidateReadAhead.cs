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

using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Deletes
{
    // Opens a record group's candidates ahead of their turn
    internal static class CandidateReadAhead
    {
        public static CommitReadAhead Create(IFileStorage storage, IOPath table, List<int> candidates, int start, int end,
            Dictionary<int, DeltaFileRecord> records, int depth, CancellationToken stop)
        {
            return new CommitReadAhead(storage, index =>
            {
                // A missing record throws at its own turn
                if (!records.TryGetValue(candidates[start + (int)index], out var record))
                {
                    return null;
                }
                // A file without a listed size waits for its turn
                return new IOEntry(table.Combine(record.Path)) { Size = record.Size > 0 ? record.Size : CommitReadAhead.WindowBytes };
            }, end - start, depth, null, () => !stop.IsCancellationRequested, CancellationToken.None);
        }
    }
}
