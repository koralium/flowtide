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
using Microsoft.Extensions.Logging;
using System.Collections;

namespace FlowtideDotNet.Core.Engine
{
    internal class LoggerCheckFailureListener : ICheckFailureListener, ICheckStatusListener
    {
        private const string FailedPrefix = "Check failed: ";
        private const string ResolvedPrefix = "Check resolved: ";
        private const string PassedTemplate = "Check passed: {CheckName}";
        private const string FailingTemplate = "Check failed: {CheckName}, {ActiveIssues} issues, {FailingRows} failing rows";
        private const string NotEvaluatedTemplate = "Check not evaluated yet: {CheckName}";
        private const string CheckIdProperty = "CheckId";

        private static readonly EventId s_checkFailedEvent = new EventId(1, "CheckFailed");
        private static readonly EventId s_checkResolvedEvent = new EventId(2, "CheckResolved");
        private static readonly EventId s_checkStatusEvent = new EventId(3, "CheckStatus");
        private static readonly EventId s_checkResetEvent = new EventId(4, "CheckReset");
        private static readonly EventId s_checkNotEvaluatedEvent = new EventId(5, "CheckNotEvaluated");

        private readonly ILogger _logger;
        private readonly LogLevel _logLevel;

        public LoggerCheckFailureListener(ILogger logger, LogLevel logLevel)
        {
            _logger = logger;
            _logLevel = logLevel;
        }

        /// <inheritdoc/>
        public void OnCheckFailure(ref readonly CheckFailureNotification notification)
        {
            LogIssue(s_checkFailedEvent, FailedPrefix, in notification);
        }

        /// <inheritdoc/>
        public void OnCheckResolved(ref readonly CheckFailureNotification notification)
        {
            LogIssue(s_checkResolvedEvent, ResolvedPrefix, in notification);
        }

        /// <inheritdoc/>
        public void OnCheckReset(ref readonly CheckResetNotification notification)
        {
            _logger.LogDebug(s_checkResetEvent, "Check reset: {CheckName}, {CheckId}", notification.CheckName, notification.CheckId);
        }

        /// <inheritdoc/>
        public void OnCheckStatus(ref readonly CheckStatusNotification notification)
        {
            // Not evaluated logs at debug to keep startup quiet
            var notEvaluated = notification.State == CheckState.NotEvaluated;
            var logLevel = notEvaluated ? LogLevel.Debug : LogLevel.Information;
            if (!_logger.IsEnabled(logLevel))
            {
                return;
            }
            var state = new CheckStatusLog(notification.CheckId, notification.CheckName, notification.State, notification.ActiveIssues, notification.FailingRows);
            _logger.Log(logLevel, notEvaluated ? s_checkNotEvaluatedEvent : s_checkStatusEvent, state, default, static (state, exception) => state.ToString());
        }

        private void LogIssue(EventId eventId, string prefix, ref readonly CheckFailureNotification notification)
        {
            if (!_logger.IsEnabled(_logLevel))
            {
                return;
            }
            var state = new CheckFailureLog(prefix, notification.CheckId, notification.CheckName, notification.Tags.ToArray());
            _logger.Log(_logLevel, eventId, state, default, static (state, exception) => state.ToString());
        }

        private readonly struct CheckFailureLog : IReadOnlyList<KeyValuePair<string, object?>>
        {
            private readonly string _prefix;
            private readonly string? _checkId;
            private readonly string _checkName;
            private readonly KeyValuePair<string, object?>[] _tags;

            public CheckFailureLog(string prefix, string checkId, string checkName, KeyValuePair<string, object?>[] tags)
            {
                _prefix = prefix;
                _checkName = checkName;
                _tags = tags;
                // A user tag named CheckId keeps its value
                _checkId = HasTag(tags, CheckIdProperty) ? null : checkId;
            }

            private static bool HasTag(KeyValuePair<string, object?>[] tags, string key)
            {
                for (int i = 0; i < tags.Length; i++)
                {
                    if (string.Equals(tags[i].Key, key, StringComparison.Ordinal))
                    {
                        return true;
                    }
                }
                return false;
            }

            public KeyValuePair<string, object?> this[int index]
            {
                get
                {
                    if (index < _tags.Length)
                    {
                        return _tags[index];
                    }
                    if (index == Count - 1)
                    {
                        return new KeyValuePair<string, object?>("{OriginalFormat}", _prefix + _checkName);
                    }
                    return new KeyValuePair<string, object?>(CheckIdProperty, _checkId);
                }
            }

            public int Count => _tags.Length + (_checkId == null ? 1 : 2);

            public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
            {
                for (int i = 0; i < Count; ++i)
                {
                    yield return this[i];
                }
            }

            public override string ToString()
            {
                return _prefix + CheckNameRenderer.Render(_checkName, _tags);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        private readonly struct CheckStatusLog : IReadOnlyList<KeyValuePair<string, object?>>
        {
            private readonly string _checkId;
            private readonly string _checkName;
            private readonly CheckState _state;
            private readonly long _activeIssues;
            private readonly long _failingRows;

            public CheckStatusLog(string checkId, string checkName, CheckState state, long activeIssues, long failingRows)
            {
                _checkId = checkId;
                _checkName = checkName;
                _state = state;
                _activeIssues = activeIssues;
                _failingRows = failingRows;
            }

            public KeyValuePair<string, object?> this[int index]
            {
                get
                {
                    if (_state == CheckState.NotEvaluated)
                    {
                        // No counts before the first evaluation
                        return index switch
                        {
                            0 => new KeyValuePair<string, object?>("CheckName", _checkName),
                            1 => new KeyValuePair<string, object?>(CheckIdProperty, _checkId),
                            2 => new KeyValuePair<string, object?>("{OriginalFormat}", NotEvaluatedTemplate),
                            _ => throw new ArgumentOutOfRangeException(nameof(index))
                        };
                    }
                    return index switch
                    {
                        0 => new KeyValuePair<string, object?>("CheckName", _checkName),
                        1 => new KeyValuePair<string, object?>("ActiveIssues", _activeIssues),
                        2 => new KeyValuePair<string, object?>("FailingRows", _failingRows),
                        3 => new KeyValuePair<string, object?>(CheckIdProperty, _checkId),
                        4 => new KeyValuePair<string, object?>("{OriginalFormat}", _state == CheckState.Passed ? PassedTemplate : FailingTemplate),
                        _ => throw new ArgumentOutOfRangeException(nameof(index))
                    };
                }
            }

            public int Count => _state == CheckState.NotEvaluated ? 3 : 5;

            public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
            {
                for (int i = 0; i < Count; ++i)
                {
                    yield return this[i];
                }
            }

            public override string ToString()
            {
                return _state switch
                {
                    CheckState.NotEvaluated => $"Check not evaluated yet: {_checkName}",
                    CheckState.Passed => $"Check passed: {_checkName}",
                    _ => $"Check failed: {_checkName}, {_activeIssues} issues, {_failingRows} failing rows"
                };
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }
}
