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
using FlowtideDotNet.Core.Engine;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Core;
using Serilog.Events;
using Serilog.Extensions.Logging;

namespace FlowtideDotNet.Core.Tests.Operators.Check
{
    public class LoggerCheckFailureListenerTests
    {
        private sealed class CapturingSink : ILogEventSink
        {
            public List<LogEvent> Events { get; } = new List<LogEvent>();

            public void Emit(LogEvent logEvent)
            {
                Events.Add(logEvent);
            }
        }

        private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger
        {
            public List<(EventId EventId, List<KeyValuePair<string, object?>> State, string Message)> Entries { get; } = new();

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
                var properties = new List<KeyValuePair<string, object?>>();
                if (state is IEnumerable<KeyValuePair<string, object?>> pairs)
                {
                    properties.AddRange(pairs);
                }
                Entries.Add((eventId, properties, formatter(state, exception)));
            }
        }

        // Disposing the provider also disposes the Serilog logger
        private static (LoggerCheckFailureListener Listener, CapturingSink Sink, SerilogLoggerProvider Serilog) CreateWithSerilog()
        {
            var sink = new CapturingSink();
            var serilog = new LoggerConfiguration().MinimumLevel.Verbose().WriteTo.Sink(sink).CreateLogger();
            var provider = new SerilogLoggerProvider(serilog, dispose: true);
            return (new LoggerCheckFailureListener(provider.CreateLogger("checks"), LogLevel.Warning), sink, provider);
        }

        private static (int Id, string? Name) EventIdOf(LogEvent logEvent)
        {
            var structure = Assert.IsType<StructureValue>(logEvent.Properties["EventId"]);
            int id = 0;
            string? name = null;
            foreach (var property in structure.Properties)
            {
                var value = ((ScalarValue)property.Value).Value;
                if (property.Name == "Id")
                {
                    id = (int)value!;
                }
                else if (property.Name == "Name")
                {
                    name = (string?)value;
                }
            }
            return (id, name);
        }

        private static object? ScalarOf(LogEvent logEvent, string propertyName)
        {
            return Assert.IsType<ScalarValue>(logEvent.Properties[propertyName]).Value;
        }

        [Fact]
        public void RaiseAndResolveDifferInEventIdAndTemplate()
        {
            var (listener, sink, serilog) = CreateWithSerilog();
            using var _ = serilog;
            string streamName = "stream";
            var tags = new KeyValuePair<string, object?>[] { new("userkey", 17L) };
            var notification = new CheckFailureNotification(ref streamName, "1:0", "User {userkey} has no company", tags);

            listener.OnCheckFailure(in notification);
            listener.OnCheckResolved(in notification);

            Assert.Equal(2, sink.Events.Count);
            var raised = sink.Events[0];
            var resolved = sink.Events[1];

            Assert.Equal((1, "CheckFailed"), EventIdOf(raised));
            Assert.Equal((2, "CheckResolved"), EventIdOf(resolved));
            Assert.Equal("Check failed: User {userkey} has no company", raised.MessageTemplate.Text);
            Assert.Equal("Check resolved: User {userkey} has no company", resolved.MessageTemplate.Text);
            Assert.Equal(LogEventLevel.Warning, raised.Level);
            Assert.Equal(LogEventLevel.Warning, resolved.Level);

            Assert.Equal(17L, ScalarOf(raised, "userkey"));
            Assert.Equal("1:0", ScalarOf(raised, "CheckId"));
            Assert.Equal(17L, ScalarOf(resolved, "userkey"));
            Assert.Equal("Check failed: User 17 has no company", raised.RenderMessage());
        }

        [Fact]
        public void FormatterRendersPrefixAndTags()
        {
            var logger = new CapturingLogger();
            var listener = new LoggerCheckFailureListener(logger, LogLevel.Warning);
            string streamName = "stream";
            var tags = new KeyValuePair<string, object?>[] { new("userkey", 17L) };
            var notification = new CheckFailureNotification(ref streamName, "1:0", "User {userkey} has no company", tags);

            listener.OnCheckFailure(in notification);
            listener.OnCheckResolved(in notification);

            Assert.Equal("Check failed: User 17 has no company", logger.Entries[0].Message);
            Assert.Equal("Check resolved: User 17 has no company", logger.Entries[1].Message);
        }

        [Fact]
        public void UserCheckIdTagIsNotOverwritten()
        {
            var logger = new CapturingLogger();
            var listener = new LoggerCheckFailureListener(logger, LogLevel.Warning);
            string streamName = "stream";
            var tags = new KeyValuePair<string, object?>[] { new("CheckId", "user value") };
            var notification = new CheckFailureNotification(ref streamName, "1:0", "Bad {CheckId}", tags);

            listener.OnCheckFailure(in notification);

            var state = logger.Entries[0].State;
            var checkIds = state.Where(x => x.Key == "CheckId").ToList();
            Assert.Single(checkIds);
            Assert.Equal("user value", checkIds[0].Value);
            Assert.Single(state, x => x.Key == "{OriginalFormat}");
        }

        [Fact]
        public void StatusAndResetHaveOwnEventIds()
        {
            var (listener, sink, serilog) = CreateWithSerilog();
            using var _ = serilog;
            string streamName = "stream";

            var failing = new CheckStatusNotification(ref streamName, "1:0", "User {userkey} has no company", CheckState.Failed, 2, 5);
            listener.OnCheckStatus(in failing);
            var passing = new CheckStatusNotification(ref streamName, "1:0", "User {userkey} has no company", CheckState.Passed, 0, 0);
            listener.OnCheckStatus(in passing);
            var reset = new CheckResetNotification(ref streamName, "1:0", "User {userkey} has no company");
            listener.OnCheckReset(in reset);
            var notEvaluated = new CheckStatusNotification(ref streamName, "1:0", "User {userkey} has no company", CheckState.NotEvaluated, 0, 0);
            listener.OnCheckStatus(in notEvaluated);

            Assert.Equal(4, sink.Events.Count);

            var failed = sink.Events[0];
            Assert.Equal((3, "CheckStatus"), EventIdOf(failed));
            Assert.Equal(LogEventLevel.Information, failed.Level);
            Assert.Equal("Check failed: {CheckName}, {ActiveIssues} issues, {FailingRows} failing rows", failed.MessageTemplate.Text);
            Assert.Equal("User {userkey} has no company", ScalarOf(failed, "CheckName"));
            Assert.Equal(2L, ScalarOf(failed, "ActiveIssues"));
            Assert.Equal(5L, ScalarOf(failed, "FailingRows"));
            Assert.Equal("1:0", ScalarOf(failed, "CheckId"));

            var passed = sink.Events[1];
            Assert.Equal((3, "CheckStatus"), EventIdOf(passed));
            Assert.Equal("Check passed: {CheckName}", passed.MessageTemplate.Text);
            Assert.Equal(0L, ScalarOf(passed, "ActiveIssues"));
            Assert.Equal(0L, ScalarOf(passed, "FailingRows"));
            Assert.Equal("1:0", ScalarOf(passed, "CheckId"));

            var resetEvent = sink.Events[2];
            Assert.Equal((4, "CheckReset"), EventIdOf(resetEvent));
            Assert.Equal(LogEventLevel.Debug, resetEvent.Level);
            Assert.Equal("1:0", ScalarOf(resetEvent, "CheckId"));

            // No counts before the first evaluation
            var notEvaluatedEvent = sink.Events[3];
            Assert.Equal((5, "CheckNotEvaluated"), EventIdOf(notEvaluatedEvent));
            Assert.Equal(LogEventLevel.Debug, notEvaluatedEvent.Level);
            Assert.Equal("Check not evaluated yet: {CheckName}", notEvaluatedEvent.MessageTemplate.Text);
            Assert.Equal(new[] { "CheckId", "CheckName" }, notEvaluatedEvent.Properties.Keys.Where(x => x != "EventId" && x != "SourceContext").Order(StringComparer.Ordinal));
        }
    }
}
