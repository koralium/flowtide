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
using FlowtideDotNet.Base.Engine;

namespace FlowtideDotNet.AcceptanceTests
{
    public class NotificationRecieverTests
    {
        [Fact]
        public void NullRollbackPreservesTheRecordedException()
        {
            var receiver = new NotificationReciever(() => { });
            var streamName = nameof(NullRollbackPreservesTheRecordedException);
            var failure = new InvalidOperationException("root cause");

            receiver.OnFailure(new StreamFailureNotification(ref streamName, failure));
            receiver.OnFailure(new StreamFailureNotification(ref streamName, null));

            Assert.Equal(2, Volatile.Read(ref receiver._failureNotifications));
            Assert.True(Volatile.Read(ref receiver._error));
            Assert.Same(failure, Volatile.Read(ref receiver._exception));
        }

        [Fact]
        public void ALaterFailureReplacesTheRecordedException()
        {
            var receiver = new NotificationReciever(() => { });
            var streamName = nameof(ALaterFailureReplacesTheRecordedException);
            var later = new InvalidOperationException("later failure");

            receiver.OnFailure(new StreamFailureNotification(ref streamName, new Exception("first failure")));
            receiver.OnFailure(new StreamFailureNotification(ref streamName, later));

            Assert.Equal(2, Volatile.Read(ref receiver._failureNotifications));
            Assert.True(Volatile.Read(ref receiver._error));
            Assert.Same(later, Volatile.Read(ref receiver._exception));
        }

        [Fact]
        public void ARealExceptionCanFollowANullRollback()
        {
            var receiver = new NotificationReciever(() => { });
            var streamName = nameof(ARealExceptionCanFollowANullRollback);
            var failure = new InvalidOperationException("root cause");

            receiver.OnFailure(new StreamFailureNotification(ref streamName, null));
            receiver.OnFailure(new StreamFailureNotification(ref streamName, failure));

            Assert.Equal(2, Volatile.Read(ref receiver._failureNotifications));
            Assert.True(Volatile.Read(ref receiver._error));
            Assert.Same(failure, Volatile.Read(ref receiver._exception));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void IntentionalCrashesOnlyIncrementTheFailureCount(bool aggregate)
        {
            var receiver = new NotificationReciever(() => { });
            var streamName = nameof(IntentionalCrashesOnlyIncrementTheFailureCount);
            Exception failure = new CrashException();
            if (aggregate)
            {
                failure = new AggregateException(failure);
            }

            receiver.OnFailure(new StreamFailureNotification(ref streamName, failure));

            Assert.Equal(1, Volatile.Read(ref receiver._failureNotifications));
            Assert.False(Volatile.Read(ref receiver._error));
            Assert.Null(Volatile.Read(ref receiver._exception));
        }
    }
}
