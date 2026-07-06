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

namespace FlowtideDotNet.Base.Vertices
{
    /// <summary>
    /// Provides an output for an <see cref="IngressVertex{TData}"/> to push data and watermarks into the stream.
    /// </summary>
    /// <typeparam name="T">The type of the underlying data payload emitted by this output.</typeparam>
    /// <remarks>
    /// This class acts as a synchronized channel to the downstream <see cref="ITargetBlock{IStreamEvent}"/>. 
    /// It ensures that data and watermarks are safely emitted while respecting the operator's checkpoint locking 
    /// mechanisms and any paused execution states within the ingress lifecycle.
    /// </remarks>
    public class IngressOutput<T> : IDisposable
    {
        private readonly IngressState<T> _ingressState;
        private readonly ITargetBlock<IStreamEvent> _targetBlock;
        private bool _inLock;
        private readonly object _gateLock = new object();
        // The two gates are separate on purpose. The stop gate is set at the stop
        // checkpoint and must survive until the run is torn down, the pause gate is
        // released by a resume. With a single shared gate a resume during a stopping
        // stream would release the stop freeze and deliver data after the stop barrier.
        private volatile TaskCompletionSource? _stopGate;
        private volatile TaskCompletionSource? _pauseGate;

        internal IngressOutput(IngressState<T> ingressState, ITargetBlock<IStreamEvent> targetBlock)
        {
            _ingressState = ingressState;
            _targetBlock = targetBlock;
            _inLock = true;
        }

        /// <summary>
        /// Gets the <see cref="System.Threading.CancellationToken"/> associated with the overarching ingress vertex's lifecycle.
        /// </summary>
        public CancellationToken CancellationToken => _ingressState._tokenSource?.Token ?? throw new NotSupportedException("Token source not set before getting cancellation token");

        /// <summary>
        /// Asynchronously sends a data payload into the dataflow stream.
        /// </summary>
        /// <param name="data">The structured data element to be emitted.</param>
        /// <returns>A <see cref="Task"/> that represents the asynchronous send operation.</returns>
        public Task SendAsync(T data)
        {
            if (_stopGate != null || _pauseGate != null)
            {
                return SendAsync_WaitForResume(data);
            }
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

        private async Task SendAsync_WaitForResume(T data)
        {
            // The output is stopped, for example because the stream is paused. The data is
            // held back and sent after the resume, dropping it would look like a successful
            // send to the caller which then commits its position past the lost data.
            // The wait observes the cancellation, a send parked across a stream failure must
            // throw instead of delivering a stale batch, the rollback replays the data.
            while (true)
            {
                var gate = _stopGate ?? _pauseGate;
                if (gate == null)
                {
                    break;
                }
                await gate.Task.WaitAsync(CancellationToken);
            }
            CancellationToken.ThrowIfCancellationRequested();
            await SendAsync(data);
        }

        private async Task SendAsync_Slow(T data)
        {
            await EnterCheckpointLock();
            await _targetBlock.SendAsync(new StreamMessage<T>(data, _ingressState._currentTime), CancellationToken);
            ExitCheckpointLock();
        }

        /// <summary>
        /// Asynchronously acquires the checkpoint lock.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the asynchronous lock acquisition.</returns>
        public async Task EnterCheckpointLock()
        {
            Debug.Assert(_ingressState._checkpointLock != null, nameof(_ingressState._checkpointLock));
            await _ingressState._checkpointLock.WaitAsync(CancellationToken).ConfigureAwait(false);
            _inLock = true;
        }

        /// <summary>
        /// Releases the previously acquired checkpoint lock.
        /// </summary>
        public void ExitCheckpointLock()
        {
            Debug.Assert(_ingressState._checkpointLock != null, nameof(_ingressState._checkpointLock));

            _inLock = false;
            _ingressState._checkpointLock.Release();
        }

        internal void Stop()
        {
            lock (_gateLock)
            {
                _stopGate ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }

        internal void Pause()
        {
            lock (_gateLock)
            {
                _pauseGate ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }

        internal void Resume()
        {
            TaskCompletionSource? gate;
            lock (_gateLock)
            {
                gate = _pauseGate;
                _pauseGate = null;
            }
            gate?.TrySetResult();
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

        internal Task SendEvent(IStreamEvent streamEvent)
        {
            return _targetBlock.SendAsync(streamEvent, CancellationToken);
        }

        /// <summary>
        /// Asynchronously emits a <see cref="Watermark"/> interval into the stream to advance the downstream logical time.
        /// </summary>
        /// <param name="watermark">The structured watermark to propagate.</param>
        /// <returns>A <see cref="Task"/> that represents the asynchronous send operation.</returns>
        public Task SendWatermark(Watermark watermark)
        {
            if (_stopGate != null || _pauseGate != null)
            {
                return SendWatermark_WaitForResume(watermark);
            }
            Debug.Assert(_ingressState._vertexHandler != null, nameof(_ingressState._vertexHandler));
            watermark.SourceOperatorId = _ingressState._vertexHandler.OperatorId;
            if (_inLock)
            {
                return _targetBlock.SendAsync(watermark, CancellationToken);
            }
            return SendWatermark_Slow(watermark);
        }

        private async Task SendWatermark_WaitForResume(Watermark watermark)
        {
            // See SendAsync_WaitForResume, a dropped watermark makes downstream consumers
            // believe the data before it never completed.
            while (true)
            {
                var gate = _stopGate ?? _pauseGate;
                if (gate == null)
                {
                    break;
                }
                await gate.Task.WaitAsync(CancellationToken);
            }
            CancellationToken.ThrowIfCancellationRequested();
            await SendWatermark(watermark);
        }

        private async Task SendWatermark_Slow(Watermark watermark)
        {
            await EnterCheckpointLock();
            await _targetBlock.SendAsync(watermark, CancellationToken);
            ExitCheckpointLock();
        }

        /// <summary>
        /// Disposes the <see cref="IngressOutput{T}"/>.
        /// </summary>
        public void Dispose()
        {
            TaskCompletionSource? stopGate;
            TaskCompletionSource? pauseGate;
            lock (_gateLock)
            {
                stopGate = _stopGate;
                _stopGate = null;
                pauseGate = _pauseGate;
                _pauseGate = null;
            }
            // Parked senders wake up and observe the cancellation, every teardown path
            // cancels the token before the outputs are disposed.
            stopGate?.TrySetResult();
            pauseGate?.TrySetResult();
        }
    }
}
