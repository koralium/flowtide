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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.Memory;
using System.Collections.Immutable;
using System.Reflection;

namespace FlowtideDotNet.AcceptanceTests.MultiJoin
{
    public class PublishedDataGenerationTests
    {
        private sealed class PublicationStream : FlowtideTestStream
        {
            public PublicationStream() : base(nameof(PublishedDataGenerationTests))
            {
            }

            public void Observe(Watermark watermark) => OnWatermark(watermark);

            public void Publish(EventBatchData data) => OnDataUpdate(data);
        }

        [Fact]
        public async Task PublicationFromFailedSinkCannotClaimTheNextFailureGeneration()
        {
            await using var stream = new PublicationStream();
            // A user may be given a company, so there has to be one
            stream.GenerateCompanies(1);
            stream.GenerateUsers(1);
            var receiver = new NotificationReciever(() => { });
            typeof(FlowtideTestStream).GetField("_notificationReciever", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(stream, receiver);
            var database = (MockDatabase)typeof(FlowtideTestStream)
                .GetField("_db", BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(stream)!;
            var offsets = database.Tables.ToImmutableDictionary(
                x => x.Key,
                x => (AbstractWatermarkValue)LongWatermarkValue.Create(x.Value.CurrentOffset));
            Assert.NotEmpty(offsets);
            Assert.Contains(database.Tables.Values, x => x.CurrentOffset > 0);
            stream.Observe(new Watermark(offsets));

            var column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(7));
            using var data = new EventBatchData(new IColumn[] { column });
            stream.Publish(data);
            Assert.True(stream.PublishedDataIsCurrent());

            // The old sink has the callback before failure but invokes it after its awaited commit returns
            Action latePublication = () => stream.Publish(data);
            string streamName = stream.StreamName;
            receiver.OnFailure(new StreamFailureNotification(ref streamName, new CrashException()));
            Assert.Equal(1, stream.FailureNotificationCount);
            Assert.False(stream.PublishedDataIsCurrent());

            latePublication();

            Assert.False(stream.PublishedDataIsCurrent(),
                "An old sink callback after the failure notification must not count as a publication from the recovered stream.");
        }
    }
}
