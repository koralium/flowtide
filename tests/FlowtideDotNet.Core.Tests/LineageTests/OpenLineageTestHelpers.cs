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

using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace FlowtideDotNet.Core.Tests.LineageTests
{
    internal sealed class CapturedRequest
    {
        public required HttpMethod Method { get; init; }

        public required Uri? RequestUri { get; init; }

        public required string? MediaType { get; init; }

        public required string? CharSet { get; init; }

        public required string Body { get; init; }

        public required TaskScheduler Scheduler { get; init; }

        public string EventType => JsonDocument.Parse(Body).RootElement.GetProperty("eventType").GetString()!;

        public Guid RunId => JsonDocument.Parse(Body).RootElement.GetProperty("run").GetProperty("runId").GetGuid();
    }

    internal sealed class CapturingHttpMessageHandler : HttpMessageHandler
    {
        private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

        private readonly Func<int, CancellationToken, Task<HttpResponseMessage>> _responder;
        private readonly object _lock = new object();
        private readonly List<CapturedRequest> _requests = new List<CapturedRequest>();
        private readonly List<(int Count, TaskCompletionSource Completion)> _waiters = new List<(int Count, TaskCompletionSource Completion)>();

        public CapturingHttpMessageHandler(Func<int, HttpResponseMessage>? responder = null)
        {
            var respond = responder ?? (_ => new HttpResponseMessage(System.Net.HttpStatusCode.OK));
            _responder = (attempt, _) => Task.FromResult(respond(attempt));
        }

        // The responder sees the request token.
        public CapturingHttpMessageHandler(Func<int, CancellationToken, Task<HttpResponseMessage>> responder)
        {
            _responder = responder;
        }

        public IReadOnlyList<CapturedRequest> Requests
        {
            get
            {
                lock (_lock)
                {
                    return _requests.ToList();
                }
            }
        }

        public Task WaitForRequestsAsync(int count)
        {
            lock (_lock)
            {
                if (_requests.Count >= count)
                {
                    return Task.CompletedTask;
                }
                var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _waiters.Add((count, completion));
                return completion.Task.WaitAsync(WaitTimeout);
            }
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var scheduler = TaskScheduler.Current;
            var body = request.Content == null ? string.Empty : await request.Content.ReadAsStringAsync(cancellationToken);
            var captured = new CapturedRequest()
            {
                Method = request.Method,
                RequestUri = request.RequestUri,
                MediaType = request.Content?.Headers.ContentType?.MediaType,
                CharSet = request.Content?.Headers.ContentType?.CharSet,
                Body = body,
                Scheduler = scheduler
            };

            int attempt;
            lock (_lock)
            {
                attempt = _requests.Count;
                _requests.Add(captured);
                for (int i = _waiters.Count - 1; i >= 0; i--)
                {
                    if (_requests.Count >= _waiters[i].Count)
                    {
                        _waiters[i].Completion.TrySetResult();
                        _waiters.RemoveAt(i);
                    }
                }
            }
            return await _responder(attempt, cancellationToken);
        }
    }

    internal sealed record LogEntry(string Category, LogLevel Level, string Message, Exception? Exception);

    internal sealed class ListLoggerProvider : ILoggerProvider
    {
        private readonly object _lock = new object();
        private readonly List<LogEntry> _entries = new List<LogEntry>();
        private readonly List<string> _categories = new List<string>();

        public IReadOnlyList<LogEntry> Entries
        {
            get
            {
                lock (_lock)
                {
                    return _entries.ToList();
                }
            }
        }

        public IReadOnlyList<string> Categories
        {
            get
            {
                lock (_lock)
                {
                    return _categories.ToList();
                }
            }
        }

        public ILogger CreateLogger(string categoryName)
        {
            lock (_lock)
            {
                _categories.Add(categoryName);
            }
            return new ListLogger(this, categoryName);
        }

        public void Dispose()
        {
        }

        private void Add(LogEntry entry)
        {
            lock (_lock)
            {
                _entries.Add(entry);
            }
        }

        private sealed class ListLogger : ILogger
        {
            private readonly ListLoggerProvider _provider;
            private readonly string _category;

            public ListLogger(ListLoggerProvider provider, string category)
            {
                _provider = provider;
                _category = category;
            }

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull
            {
                return null;
            }

            public bool IsEnabled(LogLevel logLevel)
            {
                return true;
            }

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                _provider.Add(new LogEntry(_category, logLevel, formatter(state, exception), exception));
            }
        }
    }
}
