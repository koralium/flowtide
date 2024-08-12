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

using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices.Ingress
{
    public class IngressOutput<T>
    {
        private readonly IngressState<T> _ingressState;
        private readonly ITargetBlock<IStreamEvent> _targetBlock;
        private bool _inLock;

        internal IngressOutput(IngressState<T> ingressState, ITargetBlock<IStreamEvent> targetBlock) 
        {
            _ingressState = ingressState;
            _targetBlock = targetBlock;
            _inLock = true;
        }

        public CancellationToken CancellationToken => _ingressState._tokenSource?.Token ?? throw new NotSupportedException("Token source not set before getting cancellation token");

        public Task SendAsync(T data)
        {
            if (data is IRentable rentable)
            {
                rentable.Rent(_ingressState._linkCount);
            }
            if (_inLock)
            {
                return _targetBlock.SendAsync(new StreamMessage<T>(data, _ingressState._currentTime), CancellationToken);
            }
            return SendAsync_Slow(data);
        }

        private async Task SendAsync_Slow(T data)
        {
            await EnterCheckpointLock();
            await _targetBlock.SendAsync(new StreamMessage<T>(data, _ingressState._currentTime), CancellationToken);
            ExitCheckpointLock();
        }

        public async Task EnterCheckpointLock()
        {
            Debug.Assert(_ingressState._checkpointLock != null, nameof(_ingressState._checkpointLock));
            await _ingressState._checkpointLock.WaitAsync(CancellationToken).ConfigureAwait(false);
            _inLock = true;
        }

        public void ExitCheckpointLock()
        {
            Debug.Assert(_ingressState._checkpointLock != null, nameof(_ingressState._checkpointLock));

            _inLock = false;
            _ingressState._checkpointLock.Release();
        }

        internal void Fault(Exception exception)
        {
            _targetBlock.Fault(exception);
        }

        internal Task SendCheckpoint(ICheckpointEvent checkpointEvent)
        {
            return _targetBlock.SendAsync(checkpointEvent, CancellationToken);
        }

        internal Task SendLockingEvent(ILockingEvent lockingEvent)
        {
            return _targetBlock.SendAsync(lockingEvent, CancellationToken);
        }

        public Task SendWatermark(Watermark watermark)
        {
            Debug.Assert(_ingressState._vertexHandler != null, nameof(_ingressState._vertexHandler));
            watermark.SourceOperatorId = _ingressState._vertexHandler.OperatorId;
            if (_inLock)
            {
                return _targetBlock.SendAsync(watermark, CancellationToken);
            }
            return SendWatermark_Slow(watermark);
        }

        private async Task SendWatermark_Slow(Watermark watermark)
        {
            await EnterCheckpointLock();
            await _targetBlock.SendAsync(watermark, CancellationToken);
            ExitCheckpointLock();
        }
    }
}
