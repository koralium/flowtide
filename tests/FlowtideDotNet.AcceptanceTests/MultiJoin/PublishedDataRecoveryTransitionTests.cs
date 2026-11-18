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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.Memory;
using System.Collections.Immutable;
using System.Reflection;

namespace FlowtideDotNet.AcceptanceTests.MultiJoin
{
    public class PublishedDataRecoveryTransitionTests
    {
        private sealed class PublicationStream : FlowtideTestStream
        {
            public PublicationStream() : base(nameof(PublishedDataRecoveryTransitionTests)) { }
            public void Observe(Watermark watermark) => OnWatermark(watermark);
            public void Publish(EventBatchData data) => OnDataUpdate(data);
        }

        [Fact]
        public async Task StalePublicationRemainsInvalidWhileRunningGenerationChanges()
        {
            await using var stream = new PublicationStream();
            stream.AddUser(new FlowtideDotNet.AcceptanceTests.Entities.User { UserKey = 1, FirstName = "review" });
            var receiver = new NotificationReciever(() => { });
            typeof(FlowtideTestStream).GetField("_notificationReciever", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(stream, receiver);
            var database = (MockDatabase)typeof(FlowtideTestStream)
                .GetField("_db", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(stream)!;
            var offsets = database.Tables.ToImmutableDictionary(x => x.Key,
                x => (AbstractWatermarkValue)LongWatermarkValue.Create(x.Value.CurrentOffset));
            Assert.NotEmpty(offsets);
            Assert.Contains(database.Tables.Values, x => x.CurrentOffset > 0);
            string name = stream.StreamName;
            var running = StreamStateValue.Running;
            receiver.OnStreamStateChange(new StreamStateChangeNotification(ref name, ref running));
            stream.Observe(new Watermark(offsets));
            var column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(7));
            using var data = new EventBatchData(new IColumn[] { column });
            stream.Publish(data);
            Assert.True(stream.PublishedDataIsCurrent());

            receiver.OnFailure(new StreamFailureNotification(ref name, new CrashException()));
            // A pre-crash checkpoint already in flight may refill the publication after Crash cleared it.
            stream.Publish(data);
            Assert.False(stream.PublishedDataIsCurrent());

            bool? currentDuringTransition = null;
            // Observe the actual receiver at the scheduling cut between its two generation stores.
            receiver.BetweenRunningUpdatesForTests = () => currentDuringTransition = stream.PublishedDataIsCurrent();
            receiver.OnStreamStateChange(new StreamStateChangeNotification(ref name, ref running));
            receiver.BetweenRunningUpdatesForTests = null;
            Assert.NotNull(currentDuringTransition);
            Assert.False(stream.PublishedDataIsCurrent());

            // Positive control: the first fresh publication of this run must become current.
            stream.Publish(data);
            Assert.True(stream.PublishedDataIsCurrent());
            Assert.False(currentDuringTransition.Value,
                "The old run's publication became current between the recovery-generation stores, before any fresh publication.");
        }
    }
}
