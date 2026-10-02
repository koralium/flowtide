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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Net.Http.Headers;
using System.Threading.Channels;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal sealed class OpenLineageHttpReporter : IStreamStateChangeListener
    {
        // Shared by every stream, pooled connections follow DNS.
        private static readonly HttpClient s_httpClient = new HttpClient(new SocketsHttpHandler()
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(2)
        });

        private readonly object _lock = new object();
        private readonly ILogger _logger;
        private readonly OpenLineageEvent _ev;
        private readonly OpenLineageHttpOptions _openLineageOptions;
        private readonly HttpClient _httpClient;
        private readonly Func<int, TimeSpan> _retryDelay;
        private readonly string _url;
        private readonly Channel<OpenLineageEvent> _channel;
        private StreamStateValue _previousState;
        private int _errorCount;

        internal static OpenLineageHttpReporter Create(
            ILoggerFactory? loggerFactory,
            StreamLineage lineage,
            OpenLineageHttpOptions openLineageOptions)
        {
            ILogger logger = loggerFactory != null ? loggerFactory.CreateLogger<OpenLineageHttpReporter>() : NullLogger.Instance;
            var ev = LineageEventCreator.CreateFromLineage(openLineageOptions.RunId ?? Guid.NewGuid(), lineage, openLineageOptions.IncludeSchema);
            return new OpenLineageHttpReporter(logger, ev, openLineageOptions, s_httpClient, DefaultRetryDelay);
        }

        internal OpenLineageHttpReporter(
            ILogger logger,
            OpenLineageEvent ev,
            OpenLineageHttpOptions openLineageOptions,
            HttpClient httpClient,
            Func<int, TimeSpan> retryDelay)
        {
            _url = ValidateOptions(openLineageOptions);
            _logger = logger;
            _ev = ev;
            _openLineageOptions = openLineageOptions;
            _httpClient = httpClient;
            _retryDelay = retryDelay;
            // Writers never run the reader inline.
            _channel = Channel.CreateUnbounded<OpenLineageEvent>(new UnboundedChannelOptions()
            {
                SingleReader = true,
                AllowSynchronousContinuations = false
            });
            // Task.Run escapes the Orleans grain scheduler.
            ReportingTask = Task.Run(ReportingLoop);
        }

        internal Task ReportingTask { get; }

        internal static string ValidateOptions(OpenLineageHttpOptions openLineageOptions)
        {
            if (openLineageOptions.Url == null)
            {
                throw new ArgumentException("OpenLineageOptions.Url must be set");
            }
            return openLineageOptions.Url;
        }

        internal static TimeSpan DefaultRetryDelay(int errorCount)
        {
            return TimeSpan.FromSeconds(Math.Min(15, errorCount));
        }

        internal static bool TryMapState(StreamStateValue previousState, StreamStateValue state, out LineageEventType eventType)
        {
            switch (state)
            {
                case StreamStateValue.Starting:
                    eventType = LineageEventType.Start;
                    return true;
                case StreamStateValue.Running:
                    eventType = LineageEventType.Running;
                    return true;
                case StreamStateValue.Failure:
                    eventType = LineageEventType.Fail;
                    return true;
                case StreamStateValue.NotStarted when previousState == StreamStateValue.Stopping:
                    eventType = LineageEventType.Complete;
                    return true;
                default:
                    eventType = default;
                    return false;
            }
        }

        public void OnStreamStateChange(StreamStateChangeNotification notification)
        {
            // No I/O, running fires under the checkpoint lock.
            lock (_lock)
            {
                if (TryMapState(_previousState, notification.State, out var eventType))
                {
                    _channel.Writer.TryWrite(_ev.ChangeEventType(eventType, DateTime.UtcNow));
                }
                _previousState = notification.State;
            }
        }

        private async Task ReportingLoop()
        {
            // Writer never completes, the loop outlives COMPLETE.
            await foreach (var ev in _channel.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                try
                {
                    await SendWithRetry(ev).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // Not transient, retrying would block later events.
                    _logger.LogError(ex, "Unexpected error while writing the OpenLineage {EventType} event, the event is dropped.", ev.EventType);
                }
            }
        }

        private async Task SendWithRetry(OpenLineageEvent ev)
        {
            var body = OpenLineageSerializer.SerializeToUtf8Bytes(ev);
            while (true)
            {
                Exception? error = null;
                var statusCode = "no response";
                try
                {
                    // A request message cannot be sent twice.
                    using var message = new HttpRequestMessage(HttpMethod.Post, _url);
                    message.Content = new ByteArrayContent(body);
                    message.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };
                    _openLineageOptions.OnRequest?.Invoke(message);
                    using var response = await _httpClient.SendAsync(message).ConfigureAwait(false);
                    if (response.IsSuccessStatusCode)
                    {
                        _errorCount = 0;
                        return;
                    }
                    statusCode = response.StatusCode.ToString();
                }
                catch (HttpRequestException ex)
                {
                    error = ex;
                }
                catch (OperationCanceledException ex)
                {
                    // No token passed, so this is a timeout.
                    error = ex;
                }

                _errorCount++;
                var waitTime = _retryDelay(_errorCount);
                _logger.LogError(error, "Error writing to OpenLineage destination, status code: '{StatusCode}', waiting {WaitSeconds} seconds before retrying", statusCode, waitTime.TotalSeconds);
                await Task.Delay(waitTime).ConfigureAwait(false);
            }
        }
    }
}
