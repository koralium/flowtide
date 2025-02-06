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
using FlowtideDotNet.Base.Engine.Internal.StateMachine;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    internal class NotificationReciever : ICheckpointListener, IStreamStateChangeListener, IFailureListener
    {
        private readonly Action onCheckpointComplete;
        internal Exception? _exception;
        internal bool _error;

        public NotificationReciever(Action onCheckpointComplete)
        {
            this.onCheckpointComplete = onCheckpointComplete;
        }

        public void OnCheckpointComplete()
        {
            onCheckpointComplete();
        }

        private bool IsCrashException(Exception? exception)
        {
            if (exception == null)
            {
                return false;
            }
            if (exception is CrashException)
            {
                return true;
            }
            if (exception is AggregateException aggregate)
            {
                return aggregate.InnerExceptions.Any(IsCrashException);
            }
            return false;
        }

        public void OnFailure(Exception? exception)
        {
            if (IsCrashException(exception))
            {
                return;
            }
            _error = true;
            _exception = exception;
        }

        public void OnStreamStateChange(StreamStateValue newState)
        {

        }
    }
}
