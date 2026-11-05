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
using System.Collections.Concurrent;

namespace FlowtideDotNet.Connector.SpiceDB.Tests
{
    // Keeps formatted log lines for assertions
    internal sealed class TestLogCollector : ILoggerProvider
    {
        private readonly ConcurrentQueue<(LogLevel Level, string Message)> _lines = new ConcurrentQueue<(LogLevel, string)>();

        public IReadOnlyList<string> Lines(LogLevel level, string contains)
        {
            return _lines.Where(x => x.Level == level && x.Message.Contains(contains, StringComparison.Ordinal)).Select(x => x.Message).ToList();
        }

        public ILogger CreateLogger(string categoryName)
        {
            return new Collector(this);
        }

        public void Dispose()
        {
        }

        private sealed class Collector : ILogger
        {
            private readonly TestLogCollector _owner;

            public Collector(TestLogCollector owner)
            {
                _owner = owner;
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
                _owner._lines.Enqueue((logLevel, formatter(state, exception)));
            }
        }
    }
}
