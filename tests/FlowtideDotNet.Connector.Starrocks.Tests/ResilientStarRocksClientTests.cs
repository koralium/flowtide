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

using FlowtideDotNet.Connector.StarRocks.Exceptions;
using FlowtideDotNet.Connector.StarRocks.Internal;
using FlowtideDotNet.Connector.StarRocks.Internal.HttpApi;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace FlowtideDotNet.Connector.StarRocks.Tests
{
    public class ResilientStarRocksClientTests
    {
        private static readonly StarRocksStreamLoadInfo Load = new StarRocksStreamLoadInfo("db", "table", new byte[] { 1 });

        [Fact]
        public async Task FailedRequestIsRetriedAndEachAttemptLogged()
        {
            var inner = Substitute.For<IStarrocksClient>();
            inner.StreamLoad(default).ReturnsForAnyArgs(
                Task.FromException(new HttpRequestException("Connection reset")),
                Task.FromException(new HttpRequestException("Connection reset")),
                Task.CompletedTask);
            var logger = new RecordingLogger();

            await CreateClient(inner, logger).StreamLoad(Load);

            await inner.ReceivedWithAnyArgs(3).StreamLoad(default);
            Assert.Equal(2, logger.Entries.Count);
            Assert.All(logger.Entries, entry =>
            {
                Assert.Equal(LogLevel.Warning, entry.Level);
                Assert.IsType<HttpRequestException>(entry.Exception);
                Assert.Contains("`StreamLoad`", entry.Message);
            });
            Assert.Contains("attempt 1,", logger.Entries[0].Message);
            Assert.Contains("attempt 2,", logger.Entries[1].Message);
        }

        [Fact]
        public async Task GivesUpAfterFiveRetries()
        {
            var inner = Substitute.For<IStarrocksClient>();
            inner.StreamLoad(default).ReturnsForAnyArgs(_ => Task.FromException(new HttpRequestException("Connection refused")));
            var logger = new RecordingLogger();

            await Assert.ThrowsAsync<HttpRequestException>(() => CreateClient(inner, logger).StreamLoad(Load));

            // First attempt plus five retries
            await inner.ReceivedWithAnyArgs(6).StreamLoad(default);
            Assert.Equal(6, logger.Entries.Count);
        }

        [Fact]
        public async Task RejectedRowsAreNotRetried()
        {
            var inner = Substitute.For<IStarrocksClient>();
            inner.StreamLoad(default).ReturnsForAnyArgs(_ => Task.FromException(new StarRocksInvalidDataException("too many filtered rows")));
            var logger = new RecordingLogger();

            await Assert.ThrowsAsync<StarRocksInvalidDataException>(() => CreateClient(inner, logger).StreamLoad(Load));

            await inner.ReceivedWithAnyArgs(1).StreamLoad(default);
            Assert.Single(logger.Entries);
        }

        [Fact]
        public async Task HttpClientTimeoutIsRetried()
        {
            var inner = Substitute.For<IStarrocksClient>();
            var ok = new TransactionInfo() { Status = StarRocksStatus.Ok, Message = "ok" };
            inner.CreateTransaction(default).ReturnsForAnyArgs(
                Task.FromException<TransactionInfo>(new TaskCanceledException("Timed out", new TimeoutException())),
                Task.FromResult(ok));

            var result = await CreateClient(inner, new RecordingLogger()).CreateTransaction(new StarRocksTransactionId("db", "table", "label"));

            Assert.Same(ok, result);
            await inner.ReceivedWithAnyArgs(2).CreateTransaction(default);
        }

        [Fact]
        public async Task StoppingStreamIsNotRetriedOrLogged()
        {
            using var stopping = new CancellationTokenSource();
            var inner = Substitute.For<IStarrocksClient>();
            inner.StreamLoad(default).ReturnsForAnyArgs(_ =>
            {
                stopping.Cancel();
                return Task.FromCanceled(stopping.Token);
            });
            var logger = new RecordingLogger();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => CreateClient(inner, logger, stopping.Token).StreamLoad(Load));

            await inner.ReceivedWithAnyArgs(1).StreamLoad(default);
            Assert.Empty(logger.Entries);
        }

        private static ResilientStarRocksClient CreateClient(IStarrocksClient inner, ILogger logger, CancellationToken cancellationToken = default)
        {
            // Default retry rules without the waits
            return new ResilientStarRocksClient(inner, StarRocksSinkOptions.CreateDefaultResiliencePipeline(TimeSpan.Zero), logger, "stream", "op", cancellationToken);
        }

        private sealed class RecordingLogger : ILogger
        {
            public List<(LogLevel Level, string Message, Exception? Exception)> Entries { get; } = new();

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                lock (Entries)
                {
                    Entries.Add((logLevel, formatter(state, exception), exception));
                }
            }
        }
    }
}
