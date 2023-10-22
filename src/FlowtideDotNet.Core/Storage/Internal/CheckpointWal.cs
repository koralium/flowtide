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

using System.Collections.Concurrent;
using System.Text;
using Tenray.ZoneTree.AbstractFileStream;
using Tenray.ZoneTree.Exceptions.WAL;
using Tenray.ZoneTree.Logger;
using Tenray.ZoneTree.Options;
using Tenray.ZoneTree.Serializers;
using Tenray.ZoneTree.WAL;

namespace FlowtideDotNet.Core.Storage.Internal
{
    public interface CheckWal
    {
        Task Completed { get; }

        void Drop();

        void Dispose();
    }

    internal class CheckpointWal<TKey, TValue> : IWriteAheadLog<TKey, TValue>, CheckWal
    {
        readonly ILogger Logger;

        readonly IFileStreamProvider FileStreamProvider;

        readonly CompressedFileStream FileStream;

        readonly BinaryWriter BinaryWriter;

        readonly ISerializer<TKey> KeySerializer;

        readonly ISerializer<TValue> ValueSerializer;
        private readonly bool readExisting;

        readonly object AppendLock = new();

        volatile bool isWriterCancelled = false;

        private TaskCompletionSource _writeTask;
        private bool hasWritten = false;

        public string FilePath { get; }

        public Task Completed
        {
            get
            {
                lock (AppendLock)
                {
                    if (hasWritten)
                    {
                        return _writeTask.Task;
                    }
                    return Task.CompletedTask;
                }
            }
        }

        public CheckpointWal(
        ILogger logger,
        IFileStreamProvider fileStreamProvider,
        ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer,
        string filePath,
        WriteAheadLogOptions options,
        bool readExisting)
        {
            Logger = logger;
            FilePath = filePath;
            this.readExisting = readExisting;
            FileStream = new CompressedFileStream(
                Logger,
                fileStreamProvider,
                filePath,
                options.CompressionBlockSize,
                false,
                0,
                options.CompressionMethod,
                options.CompressionLevel);
            BinaryWriter = new BinaryWriter(FileStream, Encoding.UTF8, true);
            FileStream.Seek(0, SeekOrigin.End);
            FileStreamProvider = fileStreamProvider;
            KeySerializer = keySerializer;
            ValueSerializer = valueSerializer;
            _writeTask = new TaskCompletionSource();
        }

        ~CheckpointWal()
        {
            Dispose();
        }

        public bool EnableIncrementalBackup { get; set; }

        readonly ConcurrentQueue<QueueItem> Queue = new();

        public struct QueueItem
        {
            public TKey Key;

            public TValue Value;
            public long OpIndex;

            public QueueItem(in TKey key, in TValue value, long opIndex)
            {
                Key = key;
                Value = value;
                OpIndex = opIndex;
            }
        }

        public void Append(in TKey key, in TValue value, long opIndex)
        {
            lock (AppendLock)
            {
                hasWritten = true;
                Queue.Enqueue(new QueueItem(key, value, opIndex));
            }

        }

        public void Dispose()
        {
            FileStream.Dispose();
        }

        public void Drop()
        {
            Queue.Clear();
            FileStream.Dispose();
            if (FileStreamProvider.FileExists(FilePath))
                FileStreamProvider.DeleteFile(FilePath);
            var tailPath = FilePath + ".tail";
            if (FileStreamProvider.FileExists(tailPath))
                FileStreamProvider.DeleteFile(tailPath);
        }

        void AppendLogEntry(byte[] keyBytes, byte[] valueBytes, long opIndex)
        {
            lock (AppendLock)
            {
                if (isWriterCancelled)
                    return;
                LogEntry.AppendLogEntry(BinaryWriter, keyBytes, valueBytes, opIndex);
            }
        }

        public void MarkFrozen()
        {
            lock (this)
            {
                while (Queue.TryDequeue(out var item))
                {
                    var keyBytes = KeySerializer.Serialize(item.Key);
                    var valueBytes = ValueSerializer.Serialize(item.Value);
                    AppendLogEntry(keyBytes, valueBytes, item.OpIndex);
                }
                FileStream.WriteTail();
                if (!_writeTask.Task.IsCompleted)
                {
                    _writeTask.SetResult();
                }
            }
        }

        public WriteAheadLogReadLogEntriesResult<TKey, TValue> ReadLogEntries(bool stopReadOnException, bool stopReadOnChecksumFailure, bool sortByOpIndexes)
        {
            if (!readExisting && !hasWritten)
            {
                return new WriteAheadLogReadLogEntriesResult<TKey, TValue>()
                {
                    Keys = new List<TKey>(),
                    Success = true,
                    Values = new List<TValue>()
                };
            }
            return WriteAheadLogEntryReader.ReadLogEntries<TKey, TValue, LogEntry>(
            Logger,
            FileStream,
            stopReadOnException,
            stopReadOnChecksumFailure,
            LogEntry.ReadLogEntry,
            DeserializeLogEntry,
            sortByOpIndexes);
        }

        (bool isValid, TKey key, TValue value, long opIndex) DeserializeLogEntry(in LogEntry logEntry)
        {
            var isValid = logEntry.ValidateChecksum();
            var key = KeySerializer.Deserialize(logEntry.Key);
            var value = ValueSerializer.Deserialize(logEntry.Value);
            return (isValid, key, value, logEntry.OpIndex);
        }

        public long ReplaceWriteAheadLog(TKey[] keys, TValue[] values, bool disableBackup)
        {
            Queue.Clear();

            var existingLength = FileStream.Length;
            FileStream.SetLength(0);

            for (int i = 0; i < keys.Length; i++)
            {
                Queue.Enqueue(new QueueItem(keys[i], values[i], 0));
            }
            return 0;
        }

        public void TruncateIncompleteTailRecord(IncompleteTailRecordFoundException incompleteTailException)
        {
            lock (this)
            {
                FileStream.SetLength(incompleteTailException.RecordPosition);
            }
        }
    }
}
