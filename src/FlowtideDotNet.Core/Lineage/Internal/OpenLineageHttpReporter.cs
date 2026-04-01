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
using System.Text;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal class OpenLineageHttpReporter : IStreamStateChangeListener
    {
        private readonly CancellationTokenSource _cancellationTokenSource;
        private Task _task;
        private readonly LinkedList<OpenLineageEvent> _queue;
        private readonly object _lock = new object();
        private readonly ILogger _logger;
        private readonly OpenLineageEvent _ev;
        private readonly OpenLineageHttpOptions _openLineageOptions;
        private readonly HttpClient _httpClient;
        private readonly string _url;
        private StreamStateValue _previousState;

        internal static OpenLineageHttpReporter Create(
            ILoggerFactory? loggerFactory, 
            string streamName,
            Plan plan,
            IConnectorManager connectorManager,
            OpenLineageHttpOptions openLineageOptions)
        {
            ILogger logger = loggerFactory != null ? loggerFactory.CreateLogger<OpenLineageHttpReporter>() : NullLogger.Instance;
            var ev = LineageEventCreator.CreateFromPlan(openLineageOptions.RunId ?? Guid.NewGuid(), streamName, plan, connectorManager, openLineageOptions.IncludeSchema);
            return new OpenLineageHttpReporter(logger, ev, openLineageOptions);
        }

        private OpenLineageHttpReporter(ILogger logger, OpenLineageEvent ev, OpenLineageHttpOptions openLineageOptions)
        {
            if (openLineageOptions.Url == null)
            {
                throw new ArgumentException("OpenLineageOptions.Url must be set");
            }
            _url = openLineageOptions.Url;
            _httpClient = new HttpClient();
            _queue = new LinkedList<OpenLineageEvent>();
            _cancellationTokenSource = new CancellationTokenSource();
            StartTask();
            this._logger = logger;
            this._ev = ev;
            this._openLineageOptions = openLineageOptions;
        }

        [MemberNotNull(nameof(_task))]
        private void StartTask()
        {
            _task = Task.Factory.StartNew(ReportingLoop)
                .Unwrap()
                .ContinueWith((t) =>
                {
                    if (!(t.IsCanceled || t.IsCompletedSuccessfully))
                    {
                        _logger.LogError(t.Exception, "OpenLineageHttpReporter task failed");
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
                if (ev.EventType == LineageEventType.Complete)
                {
                    _cancellationTokenSource.Cancel();
                    _cancellationTokenSource.Dispose();
                    return;
                }

                var json = OpenLineageSerializer.Serialize(ev);
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                var message = new HttpRequestMessage(HttpMethod.Post, _url) { Content = content };
                if (_openLineageOptions.OnRequest != null)
                {
                    _openLineageOptions.OnRequest(message);
                }
                try
                {
                    await _httpClient.SendAsync(message);
                }
                catch
                {
                    _queue.AddFirst(ev);
                    throw;
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
