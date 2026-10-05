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
using FlowtideDotNet.Base.Engine.Internal;

namespace FlowtideDotNet.Base.Tests
{
    public class StreamNotificationReceiverCheckIssueTests
    {
        private const string CheckName = "chk";

        private sealed class RecordingCheckListener : ICheckFailureListener
        {
            private readonly List<string> _events;
            private readonly bool _throws;

            public RecordingCheckListener(List<string> events, bool throws = false)
            {
                _events = events;
                _throws = throws;
            }

            public void OnCheckFailure(ref readonly CheckFailureNotification notification)
            {
                Record("failure", notification.StreamName, notification.CheckId, notification.CheckName, notification.Tags);
            }

            public void OnCheckResolved(ref readonly CheckFailureNotification notification)
            {
                Record("resolved", notification.StreamName, notification.CheckId, notification.CheckName, notification.Tags);
            }

            public void OnCheckReset(ref readonly CheckResetNotification notification)
            {
                lock (_events)
                {
                    _events.Add($"reset {notification.StreamName} {notification.CheckId} {notification.CheckName}");
                }
                if (_throws)
                {
                    throw new InvalidOperationException("listener failure");
                }
            }

            private void Record(string kind, string streamName, string checkId, string checkName, ReadOnlySpan<KeyValuePair<string, object?>> tags)
            {
                var tagText = string.Empty;
                foreach (var tag in tags)
                {
                    tagText += $" {tag.Key}={tag.Value}";
                }
                lock (_events)
                {
                    _events.Add($"{kind} {streamName} {checkId} {checkName}{tagText}");
                }
                if (_throws)
                {
                    throw new InvalidOperationException("listener failure");
                }
            }
        }

        private sealed class RecordingStatusListener : ICheckStatusListener
        {
            private readonly List<string> _events;
            private readonly bool _throws;

            public RecordingStatusListener(List<string> events, bool throws = false)
            {
                _events = events;
                _throws = throws;
            }

            public void OnCheckStatus(ref readonly CheckStatusNotification notification)
            {
                var outcome = notification.State.ToString().ToLowerInvariant();
                lock (_events)
                {
                    _events.Add($"status {notification.StreamName} {notification.CheckId} {notification.CheckName} {outcome} {notification.ActiveIssues} {notification.FailingRows}");
                }
                if (_throws)
                {
                    throw new InvalidOperationException("listener failure");
                }
            }
        }

        private sealed class RecordingCheckpointListener : ICheckpointListener
        {
            private readonly List<string> _events;

            public RecordingCheckpointListener(List<string> events)
            {
                _events = events;
            }

            public void OnCheckpointComplete(StreamCheckpointNotification notification)
            {
                lock (_events)
                {
                    _events.Add($"checkpoint {notification.StreamName}");
                }
            }
        }

        private static CheckIssueBatch Batch(string checkId, long version, bool isSnapshot, CheckIssueChange[] changes, (long Active, long Rows)? status = null)
        {
            return new CheckIssueBatch()
            {
                CheckId = checkId,
                CheckName = CheckName,
                Version = version,
                IsSnapshot = isSnapshot,
                Changes = changes,
                Status = status is { } s ? CheckStatus.Evaluated(s.Active, s.Rows) : null
            };
        }

        private static CheckIssueChange Raised(string key)
        {
            return new CheckIssueChange(true, [new KeyValuePair<string, object?>("k", key)]);
        }

        private static CheckIssueChange Resolved(string key)
        {
            return new CheckIssueChange(false, [new KeyValuePair<string, object?>("k", key)]);
        }

        // Registers in reverse dispatch order so ordering tests cannot pass by registration order
        private static StreamNotificationReceiver CreateReceiver(List<string> events, bool issues = true, bool status = false, bool checkpoint = false)
        {
            var receiver = new StreamNotificationReceiver("stream");
            if (checkpoint)
            {
                receiver.AddCheckpointListener(new RecordingCheckpointListener(events));
            }
            if (status)
            {
                receiver.AddCheckStatusListener(new RecordingStatusListener(events));
            }
            if (issues)
            {
                receiver.AddCheckFailureListener(new RecordingCheckListener(events));
            }
            return receiver;
        }

        [Fact]
        public void CheckEventsArePublishedBeforeCheckpointListeners()
        {
            var events = new List<string>();
            var receiver = CreateReceiver(events, status: true, checkpoint: true);

            receiver.Enqueue(Batch("1:0", 1, false, [Raised("a")], (1, 1)));
            receiver.OnCheckpointComplete(1);

            Assert.Equal(new[] { "failure stream 1:0 chk k=a", "status stream 1:0 chk failed 1 1", "checkpoint stream" }, events);
        }

        [Fact]
        public void OnlyBatchesUpToTheCommittedVersionArePublished()
        {
            var events = new List<string>();
            var receiver = CreateReceiver(events, status: true);

            receiver.Enqueue(Batch("1:0", 1, false, [Raised("v1")]));
            receiver.Enqueue(Batch("1:0", 2, false, [Raised("v2")]));
            receiver.Enqueue(Batch("1:0", 3, false, [Raised("v3")]));
            receiver.Enqueue(Batch("1:0", 3, false, [], (0, 0)));

            receiver.OnCheckpointComplete(2);
            Assert.Equal(new[] { "failure stream 1:0 chk k=v1", "failure stream 1:0 chk k=v2" }, events);

            events.Clear();
            receiver.OnCheckpointComplete(3);
            Assert.Equal(new[] { "failure stream 1:0 chk k=v3", "status stream 1:0 chk passed 0 0" }, events);
        }

        [Fact]
        public void BatchesAndChangesArePublishedInEnqueueOrder()
        {
            var events = new List<string>();
            var receiver = CreateReceiver(events, status: true);

            receiver.Enqueue(Batch("2:0", 4, false, [Raised("b1"), Resolved("b2")], (1, 2)));
            receiver.Enqueue(Batch("1:0", 4, false, [Resolved("a1"), Raised("a2")]));
            receiver.Enqueue(Batch("2:1", 3, false, [], (0, 0)));

            receiver.OnCheckpointComplete(4);

            Assert.Equal(new[]
            {
                "failure stream 2:0 chk k=b1",
                "resolved stream 2:0 chk k=b2",
                "status stream 2:0 chk failed 1 2",
                "resolved stream 1:0 chk k=a1",
                "failure stream 1:0 chk k=a2",
                "status stream 2:1 chk passed 0 0"
            }, events);
        }

        [Fact]
        public void StatusIsDispatchedAfterResetAndChanges()
        {
            var events = new List<string>();
            // Status listener registered first, still dispatched last
            var receiver = CreateReceiver(events, status: true);

            receiver.Enqueue(Batch("1:0", 1, true, [Raised("a"), Raised("b")], (2, 5)));
            receiver.OnCheckpointComplete(1);

            Assert.Equal(new[]
            {
                "reset stream 1:0 chk",
                "failure stream 1:0 chk k=a",
                "failure stream 1:0 chk k=b",
                "status stream 1:0 chk failed 2 5"
            }, events);
        }

        [Fact]
        public void PublishDispatchesWithoutACheckpoint()
        {
            var events = new List<string>();
            var receiver = CreateReceiver(events, status: true);

            receiver.Enqueue(Batch("1:0", 1, false, [Raised("pending")], (2, 2)));
            receiver.Publish(Batch("1:0", 0, true, [Raised("a")], (1, 4)));
            // The start of a check without committed state
            receiver.Publish(new CheckIssueBatch() { CheckId = "2:0", CheckName = CheckName, Version = 0, IsSnapshot = true, Changes = [], Status = CheckStatus.NotEvaluated });

            Assert.Equal(new[]
            {
                "reset stream 1:0 chk",
                "failure stream 1:0 chk k=a",
                "status stream 1:0 chk failed 1 4",
                "reset stream 2:0 chk",
                "status stream 2:0 chk notevaluated 0 0"
            }, events);
        }

        [Theory]
        [InlineData(false, false, new string[0])]
        [InlineData(true, false, new[] { "reset stream 2:0 chk", "failure stream 2:0 chk k=b", "failure stream 1:0 chk k=issueOnly", "failure stream 1:0 chk k=a" })]
        [InlineData(false, true, new[] { "status stream 2:0 chk failed 1 1", "status stream 2:0 chk failed 2 2", "status stream 1:0 chk passed 0 0", "status stream 1:0 chk failed 1 1" })]
        [InlineData(true, true, new[]
        {
            "reset stream 2:0 chk", "failure stream 2:0 chk k=b", "status stream 2:0 chk failed 1 1", "status stream 2:0 chk failed 2 2",
            "failure stream 1:0 chk k=issueOnly", "status stream 1:0 chk passed 0 0", "failure stream 1:0 chk k=a", "status stream 1:0 chk failed 1 1"
        })]
        public void ListenerKindsSetFlagsAndKeepOnlyWhatTheyReceive(bool issues, bool status, string[] expected)
        {
            var events = new List<string>();
            var receiver = CreateReceiver(events, issues, status);
            Assert.Equal(issues, receiver.IssuesEnabled);
            Assert.Equal(status, receiver.StatusEnabled);

            receiver.Enqueue(Batch("1:0", 1, false, [Raised("issueOnly")]));
            receiver.Enqueue(Batch("1:0", 1, false, [], (0, 0)));
            receiver.Enqueue(Batch("1:0", 1, false, [Raised("a")], (1, 1)));
            receiver.Publish(Batch("2:0", 0, true, [Raised("b")], (1, 1)));
            receiver.Publish(Batch("2:0", 0, false, [], (2, 2)));
            receiver.OnCheckpointComplete(1);

            Assert.Equal(expected, events);
        }

        [Fact]
        public void EmptySnapshotOnlyResetsTheCheck()
        {
            var events = new List<string>();
            var receiver = CreateReceiver(events);

            receiver.Enqueue(Batch("1:0", 1, true, []));
            receiver.OnCheckpointComplete(1);

            Assert.Equal(new[] { "reset stream 1:0 chk" }, events);
        }

        [Fact]
        public void NotificationsCarryTheUnrenderedCheckNameAndTags()
        {
            var events = new List<string>();
            var receiver = CreateReceiver(events, status: true);

            receiver.Enqueue(new CheckIssueBatch()
            {
                CheckId = "sub/3:1",
                CheckName = "key {k} too large",
                Version = 1,
                IsSnapshot = true,
                Changes = [new CheckIssueChange(true, [new KeyValuePair<string, object?>("k", 17L), new KeyValuePair<string, object?>("user", "u1")])],
                Status = CheckStatus.Evaluated(1, 2)
            });
            receiver.OnCheckpointComplete(1);

            Assert.Equal(new[]
            {
                "reset stream sub/3:1 key {k} too large",
                "failure stream sub/3:1 key {k} too large k=17 user=u1",
                "status stream sub/3:1 key {k} too large failed 1 2"
            }, events);
        }

        [Fact]
        public void DiscardPendingRemovesOnlyThatChecksBatches()
        {
            var events = new List<string>();
            var receiver = CreateReceiver(events, status: true);

            receiver.Enqueue(Batch("1:0", 1, false, [Raised("a1")]));
            receiver.Enqueue(Batch("2:0", 1, false, [Raised("b1")]));
            receiver.Enqueue(Batch("1:0", 2, false, [], (2, 2)));
            receiver.Enqueue(Batch("2:0", 2, false, [Raised("b2")], (1, 1)));

            receiver.DiscardPending("1:0");
            receiver.OnCheckpointComplete(2);

            Assert.Equal(new[] { "failure stream 2:0 chk k=b1", "failure stream 2:0 chk k=b2", "status stream 2:0 chk failed 1 1" }, events);
        }

        [Fact]
        public void CommitCallbackRunsAtItsCommitWithoutListeners()
        {
            var receiver = new StreamNotificationReceiver("stream");
            var committed = new List<string>();
            CheckIssueBatch Marker(string checkId, long version, string name) => new CheckIssueBatch()
            {
                CheckId = checkId,
                CheckName = CheckName,
                Version = version,
                IsSnapshot = false,
                Changes = [],
                Committed = () => committed.Add(name)
            };

            receiver.Enqueue(Marker("1:0", 2, "kept"));
            receiver.Enqueue(Marker("2:0", 2, "discarded"));
            receiver.DiscardPending("2:0");

            receiver.OnCheckpointComplete(1);
            Assert.Empty(committed);

            receiver.OnCheckpointComplete(2);
            Assert.Equal(new[] { "kept" }, committed);
        }

        [Fact]
        public void BatchesEnqueuedAfterDiscardArePublished()
        {
            var events = new List<string>();
            var receiver = CreateReceiver(events);

            receiver.Enqueue(Batch("1:0", 5, false, [Raised("stale")]));
            receiver.DiscardPending("1:0");
            receiver.Enqueue(Batch("1:0", 5, true, [Raised("fresh")]));
            receiver.OnCheckpointComplete(5);

            Assert.Equal(new[] { "reset stream 1:0 chk", "failure stream 1:0 chk k=fresh" }, events);
        }

        [Fact]
        public void ThrowingListenersDoNotStopOtherListeners()
        {
            var events = new List<string>();
            var receiver = new StreamNotificationReceiver("stream");
            receiver.AddCheckFailureListener(new RecordingCheckListener(new List<string>(), throws: true));
            receiver.AddCheckStatusListener(new RecordingStatusListener(new List<string>(), throws: true));
            receiver.AddCheckFailureListener(new RecordingCheckListener(events));
            receiver.AddCheckStatusListener(new RecordingStatusListener(events));
            receiver.AddCheckpointListener(new RecordingCheckpointListener(events));

            receiver.Enqueue(Batch("1:0", 1, true, [Raised("a")]));
            receiver.Enqueue(Batch("1:0", 1, false, [Resolved("a")], (0, 0)));
            receiver.Enqueue(Batch("2:0", 1, false, [Raised("b")], (1, 3)));
            receiver.OnCheckpointComplete(1);
            receiver.Publish(Batch("3:0", 0, false, [], (0, 0)));

            Assert.Equal(new[]
            {
                "reset stream 1:0 chk",
                "failure stream 1:0 chk k=a",
                "resolved stream 1:0 chk k=a",
                "status stream 1:0 chk passed 0 0",
                "failure stream 2:0 chk k=b",
                "status stream 2:0 chk failed 1 3",
                "checkpoint stream",
                "status stream 3:0 chk passed 0 0"
            }, events);
        }

        [Fact]
        public void ConcurrentEnqueuesAreAllPublished()
        {
            var events = new List<string>();
            var receiver = CreateReceiver(events);

            Parallel.For(0, 1000, i =>
            {
                receiver.Enqueue(Batch($"{i % 8}:0", 1, false, [Raised(i.ToString())]));
            });
            receiver.OnCheckpointComplete(1);

            Assert.Equal(1000, events.Count);
        }
    }
}
