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
    internal sealed class StreamNotificationReceiver : ICheckNotificationReciever
    {
        private readonly List<ICheckpointListener> _checkpointListeners = [];
        private readonly List<IStreamStateChangeListener> _streamStateListeners = [];
        private readonly List<IFailureListener> _failureListeners = [];
        private readonly List<ICheckFailureListener> _checkFailureListeners = [];
        private string _streamName;

        public StreamNotificationReceiver(string streamName)
        {
            _streamName = streamName;
        }

        public void OnCheckpointComplete()
        {
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

        public void OnCheckFailure(in ReadOnlySpan<byte> message)
        {
            var notification = new CheckFailureNotification(ref _streamName, message);
            foreach (var listener in _checkFailureListeners)
            {
                try
                {
                    listener.OnCheckFailure(in notification);
                }
                catch
                {
                    // All errors are catched so check failure listeners cant break the stream
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
    }
}
