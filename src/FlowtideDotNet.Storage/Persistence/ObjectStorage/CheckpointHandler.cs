using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage
{
    internal class CheckpointHandler
    {
        private readonly IMemoryAllocator _memoryAllocator;
        private Channel<BlobFileWriter> _channel;
        private BlobNewCheckpoint _newCheckpoint;
        private long _nextFileId = 0;
        private object _lock = new object();
        private object _checkpointFileLock = new object();


        private object _taskLock = new object();
        private Task[]? _writeTasks;
        private CancellationTokenSource? _cancellationTokenSource;

        public CheckpointHandler(IMemoryAllocator memoryAllocator)
        {
            _channel = Channel.CreateUnbounded<BlobFileWriter>();
            _newCheckpoint = new BlobNewCheckpoint(memoryAllocator);
            this._memoryAllocator = memoryAllocator;
        }

        public async Task EnqueueFileAsync(BlobFileWriter fileWriter)
        {
            if (_writeTasks == null)
            {
                lock (_taskLock)
                {
                    if (_writeTasks == null)
                    {
                        _cancellationTokenSource = new CancellationTokenSource();
                        _writeTasks = new Task[1];
                        _writeTasks[0] = Task.Run(WriteLoop);
                    }
                }
            }
            await _channel.Writer.WriteAsync(fileWriter);
        }

        public async Task FinishCheckpoint()
        {
            if (_writeTasks == null)
            {
                throw new InvalidOperationException("Checkpoint has not been started.");
            }
            _channel.Writer.Complete();

            await Task.WhenAll(_writeTasks);

            // TODO: Write checkpoint file
        }

        private long GetNextFileId()
        {
            lock (_lock)
            {
                return _nextFileId++;
            }
        }

        private async Task WriteLoop()
        {
            Debug.Assert(_cancellationTokenSource != null);
            while (!_cancellationTokenSource.Token.IsCancellationRequested)
            {
                var wait = await _channel.Reader.WaitToReadAsync();
                if (!wait)
                {
                    break;
                }
                if (!_channel.Reader.TryRead(out var file))
                {
                    continue;
                }

                long fileId = GetNextFileId();

                var fileIds = new PrimitiveList<long>(_memoryAllocator);
                fileIds.InsertStaticRange(0, fileId, file.PageIds.Count);

                lock (_checkpointFileLock)
                {
                    _newCheckpoint.AddUpsertPages(file.PageIds, fileIds, file.PageOffsets);
                }

                var filewrite = File.OpenWrite($"blob_{fileId}.blob");
                await file.CopyToAsync(filewrite);
                filewrite.Dispose();

                file.Dispose();
                fileIds.Dispose();
            }
        }
    }
}
