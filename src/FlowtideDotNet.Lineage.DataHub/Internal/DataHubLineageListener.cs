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
using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Core.Lineage.Internal.Models;

namespace FlowtideDotNet.Lineage.DataHub.Internal
{
    internal sealed class DataHubLineageListener : IStreamLineageListener, ICheckStatusListener, IStreamStateChangeListener, IFailureListener
    {
        private readonly DataHubLineageStore _store;
        // Zero until the build registers.
        private long _generation;

        public DataHubLineageListener(DataHubLineageStore store)
        {
            _store = store;
        }

        public bool IncludeConnectorSchema => _store.IncludeConnectorSchema;

        public void OnStreamBuilt(StreamLineage lineage, string logicalStreamName)
        {
            Volatile.Write(ref _generation, _store.Register(lineage, logicalStreamName));
        }

        // Runs under the stream's context lock, so only a dictionary update.
        public void OnStreamStateChange(StreamStateChangeNotification notification)
        {
            var generation = Volatile.Read(ref _generation);
            if (generation != 0)
            {
                _store.RecordStreamState(generation, notification.State);
            }
        }

        // A stop or delete during the start fails it with one of these, any other cancellation such as a timeout is a failure.
        // Should the engine reword them, a stop during the start shows as a failure rather than a failure being hidden.
        private const string StoppedWhileStarting = "The stream was stopped while it was starting.";
        private const string DeletedWhileStarting = "The stream was deleted while it was starting.";

        public void OnFailure(StreamFailureNotification notification)
        {
            var generation = Volatile.Read(ref _generation);
            if (generation != 0)
            {
                var exception = notification.Exception;
                var cancellation = exception != null &&
                    exception.GetType() == typeof(OperationCanceledException) &&
                    (exception.Message == StoppedWhileStarting || exception.Message == DeletedWhileStarting);
                _store.RecordStreamFailure(generation, cancellation);
            }
        }

        // On the checkpoint commit path, so only a dictionary update.
        public void OnCheckStatus(ref readonly CheckStatusNotification notification)
        {
            // Without a build there is no lineage to attach the check to.
            var generation = Volatile.Read(ref _generation);
            if (generation != 0)
            {
                _store.RecordCheckStatus(generation, notification.CheckId, notification.State, notification.ActiveIssues, notification.FailingRows);
            }
        }
    }
}
