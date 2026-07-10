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

namespace FlowtideDotNet.Cluster.Orleans.Tests
{
    /// <summary>
    /// Keeps the latest debug log lines from all silos in memory and writes them to a file
    /// only when a test fails. Writing to a file while the test runs slows the streams down
    /// enough to change timing sensitive behavior, several of the failures these tests hunt
    /// only appear without that slowdown.
    /// </summary>
    internal sealed class SharedRingBufferLogger : ILoggerProvider
    {
        private const int MaxLines = 200_000;
        private static readonly ConcurrentQueue<string> _lines = new ConcurrentQueue<string>();
        private static int _count;

        public static void Append(string line)
        {
            _lines.Enqueue(line);
            if (Interlocked.Increment(ref _count) > MaxLines && _lines.TryDequeue(out _))
            {
                Interlocked.Decrement(ref _count);
            }
        }

        /// <summary>
        /// Writes the buffered lines to the given file and returns the full path.
        /// </summary>
        public static string Dump(string filePath)
        {
            File.WriteAllLines(filePath, _lines.ToArray());
            return Path.GetFullPath(filePath);
        }

        public ILogger CreateLogger(string categoryName) => new RingLogger(categoryName);

        public void Dispose()
        {
        }

        private sealed class RingLogger : ILogger
        {
            private readonly string _category;

            public RingLogger(string category)
            {
                _category = category;
            }

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                Append($"{DateTime.UtcNow:HH:mm:ss.fff} [{logLevel}] {_category} {formatter(state, exception)}{(exception != null ? " EX: " + exception : "")}");
            }
        }
    }
}
