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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.TestFramework
{
    public class StreamTestMonitor : ICheckpointListener, IFailureListener, IStreamTestMonitor
    {
        private int _currentVersion;
        private int _waitedVersion;
        private Exception? exception;
        private readonly object _lock = new object();

        public void OnCheckpointComplete(StreamCheckpointNotification notification)
        {
            lock (_lock)
            {
                _currentVersion++;
            }
        }

        public void OnFailure(StreamFailureNotification notification)
        {
            lock (_lock)
            {
                exception = notification.Exception;
            }
        }

        public async Task WaitForCheckpoint(CancellationToken cancellationToken = default)
        {
            do
            {
                cancellationToken.ThrowIfCancellationRequested();
                lock (_lock)
                {
                    if (exception != null)
                    {
                        throw exception;
                    }
                    if (_currentVersion != _waitedVersion)
                    {
                        _waitedVersion = _currentVersion;
                        return;
                    }
                    if (exception != null)
                    {
                        throw exception;
                    }
                }
                await Task.Delay(10);
            } while (true);
        }
    }
}
