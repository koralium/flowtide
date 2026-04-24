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

using System.Buffers;
using System.IO.Hashing;
using System.IO.Pipelines;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal
{
    internal class Crc64PipeReader : PipeReader
    {
        private readonly PipeReader internalReader;
        private ReadResult _readResult;
        private Crc64 _crc64calculator;
        private long _consumedCount;
        private long _dataEndIndex;
        private bool _crcDone;

        public Crc64PipeReader(PipeReader internalReader)
        {
            this.internalReader = internalReader;
            _crc64calculator = new Crc64();
            _dataEndIndex = long.MaxValue;
            _crcDone = false;
        }

        public ulong GetCrc64()
        {
            return _crc64calculator.GetCurrentHashAsUInt64();
        }

        public override void AdvanceTo(SequencePosition consumed)
        {
            AddCrc64(consumed);
            internalReader.AdvanceTo(consumed);
        }

        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
        {
            AddCrc64(consumed);
            internalReader.AdvanceTo(consumed, examined);
        }

        public void SetCrcEnd(long endIndex)
        {
            _dataEndIndex = endIndex;
        }

        private void AddCrc64(SequencePosition consumed)
        {
            if (_crcDone)
            {
                return;
            }

            ReadOnlySequence<byte> slice;
            if (_consumedCount + _readResult.Buffer.Length >= _dataEndIndex)
            {
                var endPosition = _readResult.Buffer.GetPosition(_dataEndIndex - _consumedCount);
                slice = _readResult.Buffer.Slice(_readResult.Buffer.Start, endPosition);
                _crcDone = true;
            }
            else
            {
                slice = _readResult.Buffer.Slice(_readResult.Buffer.Start, consumed);
            }
                
            var i = slice.Start;
            while(slice.TryGet(ref i, out var mem))
            {
                _crc64calculator.Append(mem.Span);
            }
            _consumedCount += slice.Length;
        }

        public override void CancelPendingRead()
        {
            internalReader.CancelPendingRead();
        }

        public override void Complete(Exception? exception = null)
        {
            internalReader.Complete(exception);
        }

        public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        {
            var resultTask = internalReader.ReadAsync(cancellationToken);
            if (resultTask.IsCompletedSuccessfully)
            {
                _readResult = resultTask.Result;
                return ValueTask.FromResult(_readResult);
            }
            return ReadAsync_Slow(resultTask);
        }

        private async ValueTask<ReadResult> ReadAsync_Slow(ValueTask<ReadResult> task)
        {
            _readResult = await task.ConfigureAwait(false);
            return _readResult;
        }

        public override bool TryRead(out ReadResult result)
        {
            if (internalReader.TryRead(out _readResult))
            {
                result = _readResult;
                return true;
            }
            result = default;
            return false;
        }
    }
}
