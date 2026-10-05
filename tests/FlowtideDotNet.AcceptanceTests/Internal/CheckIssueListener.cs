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
using System.Text;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    /// <summary>
    /// Tracks the active check issues per check scope and records listener protocol violations.
    /// </summary>
    internal sealed class CheckIssueListener : ICheckFailureListener
    {
        private readonly object _lock = new object();

        // Scope, then issue identity to rendered message
        private readonly Dictionary<string, Dictionary<string, string>> _active = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
        private readonly Dictionary<string, string> _scopeCheckNames = new Dictionary<string, string>(StringComparer.Ordinal);
        private readonly List<string> _checkNames = new List<string>();
        private readonly List<string> _checkIds = new List<string>();
        private readonly List<string> _streamNames = new List<string>();
        private readonly List<string> _violations = new List<string>();
        private int _resets;
        private int _raised;
        private int _resolved;

        public int ResetCount
        {
            get
            {
                lock (_lock)
                {
                    return _resets;
                }
            }
        }

        public int RaisedCount
        {
            get
            {
                lock (_lock)
                {
                    return _raised;
                }
            }
        }

        public int ResolvedCount
        {
            get
            {
                lock (_lock)
                {
                    return _resolved;
                }
            }
        }

        /// <summary>
        /// Rendered messages of every active issue in all scopes, sorted ordinally.
        /// </summary>
        public List<string> ActiveIssues()
        {
            lock (_lock)
            {
                var issues = new List<string>();
                foreach (var scope in _active.Values)
                {
                    issues.AddRange(scope.Values);
                }
                issues.Sort(StringComparer.Ordinal);
                return issues;
            }
        }

        /// <summary>
        /// Active issues of one check scope.
        /// </summary>
        public int ActiveIssueCount(string streamName, string checkId)
        {
            lock (_lock)
            {
                return _active.TryGetValue(Scope(streamName, checkId), out var issues) ? issues.Count : 0;
            }
        }

        /// <summary>
        /// Distinct unrendered check names that have been reset, in first seen order.
        /// </summary>
        public List<string> CheckNames()
        {
            lock (_lock)
            {
                return new List<string>(_checkNames);
            }
        }

        /// <summary>
        /// Distinct check ids that have been reset, in first seen order.
        /// </summary>
        public List<string> CheckIds()
        {
            lock (_lock)
            {
                return new List<string>(_checkIds);
            }
        }

        /// <summary>
        /// Distinct stream names that have been reset, in first seen order.
        /// </summary>
        public List<string> StreamNames()
        {
            lock (_lock)
            {
                return new List<string>(_streamNames);
            }
        }

        public List<string> Violations()
        {
            lock (_lock)
            {
                return new List<string>(_violations);
            }
        }

        public void OnCheckReset(ref readonly CheckResetNotification notification)
        {
            var scope = Scope(notification.StreamName, notification.CheckId);
            lock (_lock)
            {
                _resets++;
                if (!_checkIds.Contains(notification.CheckId))
                {
                    _checkIds.Add(notification.CheckId);
                }
                if (!_streamNames.Contains(notification.StreamName))
                {
                    _streamNames.Add(notification.StreamName);
                }
                if (!_checkNames.Contains(notification.CheckName))
                {
                    _checkNames.Add(notification.CheckName);
                }
                if (_scopeCheckNames.TryGetValue(scope, out var checkName))
                {
                    VerifyCheckName(scope, checkName, notification.CheckName);
                }
                else
                {
                    _scopeCheckNames[scope] = notification.CheckName;
                }
                _active[scope] = new Dictionary<string, string>(StringComparer.Ordinal);
            }
        }

        public void OnCheckFailure(ref readonly CheckFailureNotification notification)
        {
            var scope = Scope(notification.StreamName, notification.CheckId);
            var identity = Identity(in notification);
            var rendered = Render(in notification);
            lock (_lock)
            {
                _raised++;
                if (_scopeCheckNames.TryGetValue(scope, out var checkName))
                {
                    VerifyCheckName(scope, checkName, notification.CheckName);
                }
                if (!_active.TryGetValue(scope, out var issues))
                {
                    _violations.Add($"Failure before any reset in {scope}: {identity}");
                    issues = new Dictionary<string, string>(StringComparer.Ordinal);
                    _active[scope] = issues;
                }
                if (!issues.TryAdd(identity, rendered))
                {
                    _violations.Add($"Raised an active issue in {scope}: {identity}");
                }
            }
        }

        public void OnCheckResolved(ref readonly CheckFailureNotification notification)
        {
            var scope = Scope(notification.StreamName, notification.CheckId);
            var identity = Identity(in notification);
            lock (_lock)
            {
                _resolved++;
                if (_scopeCheckNames.TryGetValue(scope, out var checkName))
                {
                    VerifyCheckName(scope, checkName, notification.CheckName);
                }
                if (!_active.TryGetValue(scope, out var issues) || !issues.Remove(identity))
                {
                    _violations.Add($"Resolved an inactive issue in {scope}: {identity}");
                }
            }
        }

        private void VerifyCheckName(string scope, string expected, string actual)
        {
            if (!string.Equals(expected, actual, StringComparison.Ordinal))
            {
                _violations.Add($"Check name changed in {scope}: '{expected}' to '{actual}'");
            }
        }

        private static string Scope(string streamName, string checkId)
        {
            return $"{streamName}|{checkId}";
        }

        private static string Identity(ref readonly CheckFailureNotification notification)
        {
            var builder = new StringBuilder(notification.CheckName);
            foreach (var tag in notification.Tags)
            {
                builder.Append('|').Append(tag.Key).Append('=').Append(tag.Value?.ToString() ?? "<null>");
            }
            return builder.ToString();
        }

        private static string Render(ref readonly CheckFailureNotification notification)
        {
            var message = notification.CheckName;
            foreach (var tag in notification.Tags)
            {
                message = message.Replace($"{{{tag.Key}}}", tag.Value?.ToString() ?? "null", StringComparison.OrdinalIgnoreCase);
            }
            return message;
        }
    }
}
