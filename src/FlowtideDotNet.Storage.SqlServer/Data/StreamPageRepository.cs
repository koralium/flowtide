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
    internal sealed class StreamPageRepository : BaseSqlRepository
    {
        private readonly SqlServerPersistentStorageSettings _settings;
        private readonly Queue<Task> _backgroundTasks = [];
        public StreamPageRepository(StreamInfo stream, SqlServerPersistentStorageSettings settings)
            : base(stream, settings)
        {
            _settings = settings;
        }

        public override void AddStreamPage(long key, byte[] value)
        {
            base.AddStreamPage(key, value);
            if (UnpersistedPages.Count > _settings.WritePagesBulkLimit)
            {
                var pages = UnpersistedPages.ToArray();
                UnpersistedPages.Clear();
                _backgroundTasks.Enqueue(Task.Run(() => SaveStreamPagesAsync(pages)));
            }
        }

        private async Task SaveStreamPagesAsync(StreamPage[] pages)
        {
            var reader = new StreamPageDataReader(pages);
            using var connection = new SqlConnection(Settings.ConnectionString);
            await connection.OpenAsync();
            await SaveStreamPagesAsync(reader, connection);
        }

        public async Task CommitAsync()
        {
            await WaitForBackgroundTasks();
            await SaveStreamPagesAsync();
            // todo: do we need to delete old versions etc. here as well (or only on checkpoint)?
        }

        private async Task WaitForBackgroundTasks(bool throwOnError = true)
        {
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
            // todo: should a page be readable after delete (if not checkpointed)?
            // todo: if a page is deleted, does that mean that all versions of that page are deleted or can an older version still be read?
            ManagedPages.MarkDeleted(key);
            return Task.CompletedTask;
        }

        public IEnumerable<ManagedStreamPage> GetDeletedPages()
        {
            return ManagedPages.SelectMany(s => s.Value).Where(s => s.ShouldDelete);
        }

        public void RemoveDeletedPagesFromMemory(IEnumerable<ManagedStreamPage> pages)
        {
            foreach (var page in pages)
            {
                if (ManagedPages.TryGetValue(page.PageId, out var set))
                {
                    set.Remove(page);
                    if (set.Count == 0)
                    {
                        ManagedPages.Remove(page.PageId);
                    }
                }
            }
        }

        public async Task ClearLocalAndWaitForBackgroundTasks()
        {
            ClearLocal();
            await WaitForBackgroundTasks(throwOnError: false);
        }
    }
}
