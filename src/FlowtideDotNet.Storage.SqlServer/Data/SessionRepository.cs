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

using Microsoft.Data.SqlClient;

namespace FlowtideDotNet.Storage.SqlServer.Data
{
    internal sealed class SessionRepository : BaseSqlRepository
    {
        private readonly SqlServerPersistentStorageSettings _settings;
        private readonly Queue<Task> _backgroundTasks = [];

        public SessionRepository(StreamInfo stream, SqlServerPersistentStorageSettings settings)
            : base(stream, settings)
        {
            _settings = settings;
        }

        public override void AddStreamPage(long key, byte[] value)
        {
            base.AddStreamPage(key, value);
            if (UnpersistedPages.Count > _settings.WritePagesBulkLimit)
            {
#if DEBUG_WRITE
                DebugWriter!.WriteMessage($"AddStreamPage creating bg task");
#endif
                var pages = UnpersistedPages.ToArray();
                UnpersistedPages.Clear();
                _backgroundTasks.Enqueue(Task.Run(() => SaveStreamPagesAsync(pages)));
            }
        }

        private async Task SaveStreamPagesAsync(StreamPage[] pages)
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif
            var reader = new StreamPageDataReader(pages);
            using var connection = new SqlConnection(Settings.ConnectionStringFunc());
            await connection.OpenAsync();
            await SaveStreamPagesAsync(reader, connection);
        }

        public async Task CommitAsync()
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif
            await WaitForBackgroundTasks();
            await SaveStreamPagesAsync();
        }

        public async Task CommitAsync(SqlTransaction transaction)
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif
            await WaitForBackgroundTasks();
            await SaveStreamPagesAsync(transaction);
        }

        private async Task WaitForBackgroundTasks(bool throwOnError = true)
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif
            var exceptions = new List<Exception>();
            while (_backgroundTasks.TryDequeue(out var task))
            {
                try
                {
                    if (task.IsFaulted)
                    {
                        exceptions.Add(task.Exception ?? new Exception("An unknown error occured while waiting for bulk copy background task. Task is faulted."));
                    }
                    else if (!task.IsCompleted)
                    {
                        await task;
                    }
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }

            if (throwOnError && exceptions.Count > 0)
            {
                throw new AggregateException(exceptions);
            }
        }

        public Task DeleteAsync(long key)
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif
            ManagedPages.MarkPageDeleted(key);
            return Task.CompletedTask;
        }

        public void RestoreDeletedPages()
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif
            foreach (var page in GetDeletedPages())
            {
                ManagedPages.AddOrReplacePage(page.CopyAsNotDeleted());
            }
        }

        public IEnumerable<ManagedStreamPage> GetDeletedPages()
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif
            return ManagedPages.Where(s => s.Value.ShouldDelete).Select(s => s.Value);
        }

        public void RemoveDeletedPagesFromMemory(IEnumerable<ManagedStreamPage> pages)
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
            DebugWriter!.DumpObj(pages);
#endif
            foreach (var page in pages.Where(page => ManagedPages.TryGetValue(page.PageId, out var _)))
            {
                ManagedPages.Remove(page.PageId);
            }
        }

        public async Task ClearLocalAndWaitForBackgroundTasks()
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif
            ClearLocal();
            await WaitForBackgroundTasks(throwOnError: false);
        }
    }
}
