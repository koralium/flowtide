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

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    /// <summary>
    /// One received check status.
    /// </summary>
    internal readonly record struct ReportedCheckStatus(string StreamName, string CheckId, string CheckName, long ActiveIssues, long FailingRows, bool Passed);

    /// <summary>
    /// Records every check status in arrival order and checks it against the issue view when one is given.
    /// </summary>
    internal sealed class CheckStatusListener : ICheckStatusListener
    {
        private readonly object _lock = new object();
        private readonly CheckIssueListener? _issues;
        private readonly List<ReportedCheckStatus> _statuses = new List<ReportedCheckStatus>();
        private readonly List<string> _violations = new List<string>();

        /// <summary>
        /// Creates a listener, issues must be registered on the same stream when given.
        /// </summary>
        public CheckStatusListener(CheckIssueListener? issues = null)
        {
            _issues = issues;
        }

        public int Count
        {
            get
            {
                lock (_lock)
                {
                    return _statuses.Count;
                }
            }
        }

        /// <summary>
        /// Every received status in arrival order.
        /// </summary>
        public List<ReportedCheckStatus> Statuses()
        {
            lock (_lock)
            {
                return new List<ReportedCheckStatus>(_statuses);
            }
        }

        /// <summary>
        /// The last status of every check scope, keyed by stream name and check id.
        /// </summary>
        public Dictionary<(string StreamName, string CheckId), ReportedCheckStatus> Latest()
        {
            lock (_lock)
            {
                var latest = new Dictionary<(string StreamName, string CheckId), ReportedCheckStatus>();
                foreach (var status in _statuses)
                {
                    latest[(status.StreamName, status.CheckId)] = status;
                }
                return latest;
            }
        }

        public List<string> Violations()
        {
            lock (_lock)
            {
                return new List<string>(_violations);
            }
        }

        public void OnCheckStatus(ref readonly CheckStatusNotification notification)
        {
            var status = new ReportedCheckStatus(notification.StreamName, notification.CheckId, notification.CheckName, notification.ActiveIssues, notification.FailingRows, notification.Passed);
            // Issues of the same item are dispatched before its status
            var activeIssues = _issues?.ActiveIssueCount(notification.StreamName, notification.CheckId);
            lock (_lock)
            {
                if (status.Passed != (status.ActiveIssues == 0))
                {
                    _violations.Add($"Passed does not follow the active issues: {status}");
                }
                if (status.ActiveIssues < 0 || status.FailingRows < status.ActiveIssues)
                {
                    _violations.Add($"Every active issue needs a failing row: {status}");
                }
                if (activeIssues.HasValue && activeIssues.Value != status.ActiveIssues)
                {
                    _violations.Add($"The issue view has {activeIssues.Value} active issues: {status}");
                }
                _statuses.Add(status);
            }
        }
    }
}
