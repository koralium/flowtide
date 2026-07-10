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

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    /// <summary>
    /// Logger provider that keeps the latest log lines in memory. Used to debug timing
    /// sensitive tests, writing log lines to a file changes the timing enough to hide
    /// race conditions, the buffer is only written out when a test has already failed.
    /// </summary>
    internal sealed class RingBufferLoggerProvider : ILoggerProvider
    {
        private const int MaxLines = 50_000;
        private readonly ConcurrentQueue<string> _lines = new ConcurrentQueue<string>();

        public ILogger CreateLogger(string categoryName)
        {
            return new RingBufferLogger(this, categoryName);
        }

        public void Dispose()
        {
        }

        private void Add(string line)
        {
            _lines.Enqueue(line);
            while (_lines.Count > MaxLines && _lines.TryDequeue(out _))
            {
                // Intentionally empty: trimming is performed by TryDequeue in the loop condition.
            }
        }

        public void WriteToFile(string path)
        {
            var directory = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }
            File.WriteAllLines(path, _lines);
        }

        private sealed class RingBufferLogger : ILogger
        {
            private readonly RingBufferLoggerProvider _provider;
            private readonly string _categoryName;

            public RingBufferLogger(RingBufferLoggerProvider provider, string categoryName)
            {
                _provider = provider;
                _categoryName = categoryName;
            }

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull
            {
                return null;
            }

            public bool IsEnabled(LogLevel logLevel)
            {
                return logLevel >= LogLevel.Trace;
            }

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (!IsEnabled(logLevel))
                {
                    return;
                }
                var line = $"{DateTime.UtcNow:HH:mm:ss.ffffff} [{logLevel}] {formatter(state, exception)}";
                if (exception != null)
                {
                    line += Environment.NewLine + exception;
                }
                _provider.Add(line);
            }
        }
    }
}
