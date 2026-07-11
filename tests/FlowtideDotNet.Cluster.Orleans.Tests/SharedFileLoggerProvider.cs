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

namespace FlowtideDotNet.Cluster.Orleans.Tests
{
    /// <summary>
    /// Debug logger writing lines from all silos to one shared file, only used when
    /// diagnosing failing cluster tests.
    /// </summary>
    internal sealed class SharedFileLoggerProvider : ILoggerProvider
    {
        private static readonly object _lock = new object();
        private readonly string _filePath;

        public SharedFileLoggerProvider(string filePath)
        {
            _filePath = filePath;
        }

        public ILogger CreateLogger(string categoryName) => new FileLogger(this, categoryName);

        public void Dispose()
        {
        }

        private sealed class FileLogger : ILogger
        {
            private readonly SharedFileLoggerProvider _provider;
            private readonly string _category;

            public FileLogger(SharedFileLoggerProvider provider, string category)
            {
                _provider = provider;
                _category = category;
            }

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                var line = $"{DateTime.UtcNow:HH:mm:ss.fff} [{logLevel}] {_category} {formatter(state, exception)}{(exception != null ? " EX: " + exception : "")}";
                lock (_lock)
                {
                    File.AppendAllText(_provider._filePath, line + Environment.NewLine);
                }
            }
        }
    }
}
