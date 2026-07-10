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

using Orleans.Runtime;

namespace FlowtideDotNet.Cluster.Orleans.Tests
{
    /// <summary>
    /// Reminder table backed by a static dictionary shared by all silos in the test process,
    /// so reminders survive a silo stopping, like a database backed reminder table would in a
    /// real deployment. The substream keep alive reminder relies on this to reactivate
    /// substream grains that were hosted on a lost silo.
    /// </summary>
    internal sealed class SharedInMemoryReminderTable : IReminderTable
    {
        private static readonly Dictionary<(GrainId, string), ReminderEntry> _table = new Dictionary<(GrainId, string), ReminderEntry>();
        private static readonly object _lock = new object();

        public Task Init() => Task.CompletedTask;

        public Task<ReminderEntry> ReadRow(GrainId grainId, string reminderName)
        {
            lock (_lock)
            {
                _table.TryGetValue((grainId, reminderName), out var entry);
                return Task.FromResult(entry!);
            }
        }

        public Task<ReminderTableData> ReadRows(GrainId grainId)
        {
            lock (_lock)
            {
                var rows = _table.Values.Where(e => e.GrainId == grainId).ToList();
                return Task.FromResult(new ReminderTableData(rows));
            }
        }

        public Task<ReminderTableData> ReadRows(uint begin, uint end)
        {
            lock (_lock)
            {
                // Ring range semantics matching the Orleans reminder tables: the range is
                // (begin, end] and wraps around when begin >= end.
                var rows = _table.Values.Where(e =>
                {
                    var hash = e.GrainId.GetUniformHashCode();
                    return begin < end
                        ? hash > begin && hash <= end
                        : hash > begin || hash <= end;
                }).ToList();
                SharedRingBufferLogger.Append($"{DateTime.UtcNow:HH:mm:ss.fff} [Debug] ReminderTable ReadRows({begin},{end}) -> {rows.Count} of {_table.Count} rows: [{string.Join(",", rows.Select(r => r.GrainId))}]");
                return Task.FromResult(new ReminderTableData(rows));
            }
        }

        public Task<string> UpsertRow(ReminderEntry entry)
        {
            lock (_lock)
            {
                entry.ETag = Guid.NewGuid().ToString();
                _table[(entry.GrainId, entry.ReminderName)] = entry;
                SharedRingBufferLogger.Append($"{DateTime.UtcNow:HH:mm:ss.fff} [Debug] ReminderTable UpsertRow {entry.GrainId} hash {entry.GrainId.GetUniformHashCode()} start {entry.StartAt:HH:mm:ss} period {entry.Period}");
                return Task.FromResult(entry.ETag);
            }
        }

        public Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag)
        {
            lock (_lock)
            {
                if (_table.TryGetValue((grainId, reminderName), out var entry) && entry.ETag == eTag)
                {
                    _table.Remove((grainId, reminderName));
                    return Task.FromResult(true);
                }
                return Task.FromResult(false);
            }
        }

        public Task TestOnlyClearTable()
        {
            lock (_lock)
            {
                _table.Clear();
                return Task.CompletedTask;
            }
        }
    }
}
