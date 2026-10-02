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

using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Substrait;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal class OpenLineageReporter : IStreamStateChangeListener
    {
        private readonly CancellationTokenSource _cancellationTokenSource;
        private Task _task;
        private readonly LinkedList<OpenLineageEvent> _queue;
        private readonly object _lock = new object();
        private readonly ILogger _logger;
        private readonly OpenLineageEvent _ev;
        private readonly IOpenLineageTransport _transport;
        private StreamStateValue _previousState;
        private int _errorCount;

        internal static OpenLineageReporter Create(
            ILoggerFactory? loggerFactory, 
            string streamName,
            Plan plan,
            IConnectorManager connectorManager,
            OpenLineageOptions openLineageOptions,
            Func<IOpenLineageTransport> transportFactory)
        {
            ILogger logger = loggerFactory != null ? loggerFactory.CreateLogger<OpenLineageReporter>() : NullLogger.Instance;
            var ev = LineageEventCreator.CreateFromPlan(openLineageOptions.RunId ?? Guid.NewGuid(), streamName, plan, connectorManager, openLineageOptions.IncludeSchema);
            return new OpenLineageReporter(logger, ev, transportFactory());
        }

        private OpenLineageReporter(ILogger logger, OpenLineageEvent ev, IOpenLineageTransport transport)
        {
            _transport = transport;
            _queue = new LinkedList<OpenLineageEvent>();
            _cancellationTokenSource = new CancellationTokenSource();
            this._logger = logger;
            this._ev = ev;

            // Start background task
            StartTask();
        }

        [MemberNotNull(nameof(_task))]
        private void StartTask()
        {
            _task = Task.Factory.StartNew(ReportingLoop, TaskCreationOptions.LongRunning)
                .Unwrap()
                .ContinueWith((t) =>
                {
                    if (!(t.IsCanceled || t.IsCompletedSuccessfully))
                    {
                        _logger.LogError(t.Exception, "OpenLineageReporter task failed");
                        StartTask();
                    }
                });
        }

        private async Task ReportingLoop()
        {
            while (!_cancellationTokenSource.IsCancellationRequested)
            {
                OpenLineageEvent? ev = default;
                lock (_lock)
                {
                    if (_queue.Count > 0 && _queue.First != null)
                    {
                        ev = _queue.First.Value;
                        _queue.RemoveFirst();
                    }
                }
                if (ev == null)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(100));
                    continue;
                }

                var transportEvent = new OpenLineageTransportEvent(ev.Job.Namespace, ev.Job.Name, OpenLineageSerializer.Serialize(ev));
                try
                {
                    await _transport.EmitAsync(transportEvent, _cancellationTokenSource.Token);
                    _errorCount = 0;
                }
                catch (OperationCanceledException)
                {
                    // Respect cancellation requests and do not treat them as transient errors.
                    throw;
                }
                catch (Exception ex)
                {
                    _errorCount++;
                    lock (_lock)
                    {
                        _queue.AddFirst(ev);
                    }

                    TimeSpan waitTime = TimeSpan.FromSeconds(Math.Min(15, _errorCount));
                    await Task.Delay(waitTime);
                    _logger.LogError(ex, "Error writing to OpenLineage destination, waiting: {waitSeconds} seconds before retrying", waitTime.TotalSeconds);
                }

                if (ev.EventType == LineageEventType.Complete)
                {
                    _transport.Dispose();
                    _cancellationTokenSource.Cancel();
                    _cancellationTokenSource.Dispose();
                    return;
                }
            }
        }

        public void OnStreamStateChange(StreamStateChangeNotification notification)
        {
            lock (_lock)
            {
                switch(notification.State)
                {
                    case StreamStateValue.Running:
                        var e = _ev.ChangeEventType(LineageEventType.Running);
                        _queue.AddLast(e);
                        break;
                    case StreamStateValue.Starting:
                        _queue.AddLast(_ev.ChangeEventType(LineageEventType.Start));
                        break;
                    case StreamStateValue.NotStarted:
                        if (_previousState == StreamStateValue.Stopping)
                        {
                            _queue.AddLast(_ev.ChangeEventType(LineageEventType.Complete));
                        }
                        break;
                    case StreamStateValue.Failure:
                        _queue.AddLast(_ev.ChangeEventType(LineageEventType.Fail));
                        break;
                }
                _previousState = notification.State;
            }
        }
    }
}
