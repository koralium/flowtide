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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    // The guard's window, kept in stream state across restores
    internal sealed class CompactionWindow
    {
        private long _windowMs;
        private long _lastSettledAt;

        // How long a table must stay quiet, in milliseconds
        public long WindowMs
        {
            get => Volatile.Read(ref _windowMs);
            set => Volatile.Write(ref _windowMs, value);
        }

        // Unix ms when a delete last selected a settled file
        public long LastSettledAt
        {
            get => Volatile.Read(ref _lastSettledAt);
            set => Volatile.Write(ref _lastSettledAt, value);
        }
    }
}
