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

using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests.Reservoir
{
    internal class TestDataProvider : MemoryFileProvider
    {
        private TaskCompletionSource? _writeBlock;
        private TaskCompletionSource? _readBlock;
        private Func<ulong, Exception?>? _readExceptionFactory;
        private Func<ulong, Exception?>? _writeExceptionFactory;
        private TaskCompletionSource? _deleteBlock;
        private ulong? _deleteBlockTarget;
        private int _numberOfReadMemory;
        private int _numberOfReadAsync;
        private int _numberOfReadDataFile;

        public int NumberOfReadMemory => Volatile.Read(ref _numberOfReadMemory);

        public int NumberOfReadAsync => Volatile.Read(ref _numberOfReadAsync);

        public int NumberOfReadDataFile => Volatile.Read(ref _numberOfReadDataFile);

        // ── Write blocking ──────────────────────────────────────────────────────

        public void BlockWrites()
        {
            _writeBlock = new TaskCompletionSource();
        }

        public void UnblockWrites()
        {
            _writeBlock?.SetResult();
            _writeBlock = null;
        }

        // ── Read blocking (gates ReadDataFileAsync) ─────────────────────────────

        public void BlockReads()
        {
            _readBlock = new TaskCompletionSource();
        }

        public void UnblockReads()
        {
            _readBlock?.SetResult();
            _readBlock = null;
        }

        // ── Read exception injection ────────────────────────────────────────────

        /// <summary>
        /// When set, <see cref="ReadDataFileAsync"/> calls the factory for each request.
        /// If the factory returns a non-null exception that exception is thrown instead of
        /// actually reading the file. Pass <c>null</c> to remove the injection.
        /// </summary>
        public void InjectReadException(Func<ulong, Exception?>? factory)
        {
            _readExceptionFactory = factory;
        }

        public void InjectWriteException(Func<ulong, Exception?>? factory)
        {
            _writeExceptionFactory = factory;
        }

        public void InjectDeleteBlocker(ulong? fileId, TaskCompletionSource? block)
        {
            _deleteBlockTarget = fileId;
            _deleteBlock = block;
        }

        // ── Overrides ───────────────────────────────────────────────────────────

        public override ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(ulong fileId, int offset, int length, uint crc32, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _numberOfReadMemory);
            return base.GetMemoryAsync(fileId, offset, length, crc32);
        }

        public override ValueTask<T> ReadAsync<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _numberOfReadAsync);
            return base.ReadAsync(fileId, offset, length, crc32, stateSerializer);
        }

        public override async Task<PipeReader> ReadDataFileAsync(ulong fileId, int fileSize, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _numberOfReadDataFile);

            if (_readExceptionFactory?.Invoke(fileId) is { } ex)
                throw ex;

            if (_readBlock != null)
                await _readBlock.Task;

            return await base.ReadDataFileAsync(fileId, fileSize, cancellationToken);
        }

        public override async Task WriteDataFileAsync(ulong fileId, ulong crc64, int size, bool isBundle, PipeReader data, CancellationToken cancellationToken = default)
        {
            if (_writeExceptionFactory?.Invoke(fileId) is { } ex)
                throw ex;

            if (_writeBlock != null)
            {
                await _writeBlock.Task;
            }

            await base.WriteDataFileAsync(fileId, crc64, size, isBundle, data);
        }

        public override async Task DeleteDataFileAsync(ulong fileId, CancellationToken cancellationToken = default)
        {
            if (_deleteBlock != null && (_deleteBlockTarget == null || _deleteBlockTarget.Value == fileId))
            {
                await _deleteBlock.Task;
            }
            await base.DeleteDataFileAsync(fileId, cancellationToken);
        }
    }
}
