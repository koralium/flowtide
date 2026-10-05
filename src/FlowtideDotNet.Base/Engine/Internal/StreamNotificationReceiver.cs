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

using FlowtideDotNet.Base.Engine.Internal.StateMachine;

namespace FlowtideDotNet.Base.Engine.Internal
{
    internal sealed class StreamNotificationReceiver : ICheckIssuePublisher
    {
        private readonly List<ICheckpointListener> _checkpointListeners = [];
        private readonly List<IStreamStateChangeListener> _streamStateListeners = [];
        private readonly List<IFailureListener> _failureListeners = [];
        private readonly List<ICheckFailureListener> _checkFailureListeners = [];
        private readonly List<ICheckStatusListener> _checkStatusListeners = [];
        private readonly List<CheckIssueBatch> _pendingCheckIssues = [];
        private readonly object _pendingCheckIssuesLock = new object();
        // Keeps check events in commit order across publishers.
        private readonly object _publishCheckIssuesLock = new object();
        private string _streamName;

        public StreamNotificationReceiver(string streamName)
        {
            _streamName = streamName;
        }

        public void OnCheckpointComplete(long committedVersion)
        {
            // Check events land before checkpoint listeners run.
            PublishCheckIssues(committedVersion);

            var notification = new StreamCheckpointNotification(ref _streamName);
            foreach (var listener in _checkpointListeners)
            {
                try
                {
                    listener.OnCheckpointComplete(notification);
                }
                catch
                {
                    // All errors are catched so checkpoint listeners cant break the stream
                }
            }
        }

        public void OnStreamStateChange(StreamStateValue newState)
        {
            var notification = new StreamStateChangeNotification(ref _streamName, ref newState);
            foreach (var listener in _streamStateListeners)
            {
                try
                {
                    listener.OnStreamStateChange(notification);
                }
                catch
                {
                    // All errors are catched so stream state listeners cant break the stream
                }
            }
        }

        public void OnFailure(Exception? exception)
        {
            var notification = new StreamFailureNotification(ref _streamName, exception);
            foreach (var listener in _failureListeners)
            {
                try
                {
                    listener.OnFailure(notification);
                }
                catch
                {
                    // All errors are catched so failure listeners cant break the stream
                }
            }
        }

        /// <inheritdoc/>
        public bool IssuesEnabled => _checkFailureListeners.Count > 0;

        /// <inheritdoc/>
        public bool StatusEnabled => _checkStatusListeners.Count > 0;

        /// <inheritdoc/>
        public void Enqueue(CheckIssueBatch batch)
        {
            if (!CanBeReceived(batch))
            {
                // Nobody would receive it.
                return;
            }
            lock (_pendingCheckIssuesLock)
            {
                _pendingCheckIssues.Add(batch);
            }
        }

        /// <inheritdoc/>
        public void Publish(CheckIssueBatch batch)
        {
            lock (_publishCheckIssuesLock)
            {
                NotifyCheckBatch(batch);
            }
        }

        /// <inheritdoc/>
        public void DiscardPending(string checkId)
        {
            lock (_pendingCheckIssuesLock)
            {
                int kept = 0;
                for (int i = 0; i < _pendingCheckIssues.Count; i++)
                {
                    var batch = _pendingCheckIssues[i];
                    if (!string.Equals(batch.CheckId, checkId, StringComparison.Ordinal))
                    {
                        _pendingCheckIssues[kept++] = batch;
                    }
                }
                _pendingCheckIssues.RemoveRange(kept, _pendingCheckIssues.Count - kept);
            }
        }

        private bool CanBeReceived(CheckIssueBatch batch)
        {
            if (batch.Status.HasValue && _checkStatusListeners.Count > 0)
            {
                return true;
            }
            return (batch.IsSnapshot || batch.Changes.Count > 0) && _checkFailureListeners.Count > 0;
        }

        private void PublishCheckIssues(long committedVersion)
        {
            lock (_publishCheckIssuesLock)
            {
                var batches = TakeCommittedCheckIssues(committedVersion);
                if (batches == null)
                {
                    return;
                }
                foreach (var batch in batches)
                {
                    NotifyCheckBatch(batch);
                }
            }
        }

        // Reset, then issue changes, then status.
        private void NotifyCheckBatch(CheckIssueBatch batch)
        {
            if (_checkFailureListeners.Count > 0)
            {
                if (batch.IsSnapshot)
                {
                    NotifyCheckReset(batch);
                }
                var changes = batch.Changes;
                for (int i = 0; i < changes.Count; i++)
                {
                    NotifyCheckIssue(batch, changes[i]);
                }
            }
            if (batch.Status.HasValue && _checkStatusListeners.Count > 0)
            {
                NotifyCheckStatus(batch, batch.Status.Value);
            }
        }

        private List<CheckIssueBatch>? TakeCommittedCheckIssues(long committedVersion)
        {
            lock (_pendingCheckIssuesLock)
            {
                List<CheckIssueBatch>? committed = null;
                int kept = 0;
                for (int i = 0; i < _pendingCheckIssues.Count; i++)
                {
                    var batch = _pendingCheckIssues[i];
                    if (batch.Version <= committedVersion)
                    {
                        committed ??= new List<CheckIssueBatch>();
                        committed.Add(batch);
                    }
                    else
                    {
                        _pendingCheckIssues[kept++] = batch;
                    }
                }
                _pendingCheckIssues.RemoveRange(kept, _pendingCheckIssues.Count - kept);
                return committed;
            }
        }

        private void NotifyCheckReset(CheckIssueBatch batch)
        {
            var notification = new CheckResetNotification(ref _streamName, batch.CheckId, batch.CheckName);
            foreach (var listener in _checkFailureListeners)
            {
                try
                {
                    listener.OnCheckReset(in notification);
                }
                catch
                {
                    // All errors are catched so check failure listeners cant break the stream
                }
            }
        }

        private void NotifyCheckIssue(CheckIssueBatch batch, CheckIssueChange change)
        {
            var notification = new CheckFailureNotification(ref _streamName, batch.CheckId, batch.CheckName, change.Tags);
            foreach (var listener in _checkFailureListeners)
            {
                try
                {
                    if (change.Active)
                    {
                        listener.OnCheckFailure(in notification);
                    }
                    else
                    {
                        listener.OnCheckResolved(in notification);
                    }
                }
                catch
                {
                    // All errors are catched so check failure listeners cant break the stream
                }
            }
        }

        private void NotifyCheckStatus(CheckIssueBatch batch, CheckStatus status)
        {
            var notification = new CheckStatusNotification(ref _streamName, batch.CheckId, batch.CheckName, status.ActiveIssues, status.FailingRows);
            foreach (var listener in _checkStatusListeners)
            {
                try
                {
                    listener.OnCheckStatus(in notification);
                }
                catch
                {
                    // All errors are catched so check status listeners cant break the stream
                }
            }
        }

        internal void AddCheckpointListener(ICheckpointListener checkpointListener)
        {
            _checkpointListeners.Add(checkpointListener);
        }

        internal void AddStreamStateChangeListener(IStreamStateChangeListener streamStateListener)
        {
            _streamStateListeners.Add(streamStateListener);
        }

        internal void AddFailureListener(IFailureListener failureListener)
        {
            _failureListeners.Add(failureListener);
        }

        internal void AddCheckFailureListener(ICheckFailureListener checkFailureListener)
        {
            _checkFailureListeners.Add(checkFailureListener);
        }

        internal void AddCheckStatusListener(ICheckStatusListener checkStatusListener)
        {
            _checkStatusListeners.Add(checkStatusListener);
        }
    }
}
