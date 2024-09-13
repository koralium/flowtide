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

using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.Memory
{
    public class StreamMemoryManager : IStreamMemoryManager
    {
        private readonly string _streamName;
        private readonly Meter _meter;
        private readonly Dictionary<string, OperatorMemoryManager> _managers;

        public StreamMemoryManager(string streamName)
        {
            this._streamName = streamName;
            _meter = new Meter($"flowtide.{streamName}.memory");
            _managers = new Dictionary<string, OperatorMemoryManager>();
        }

        public IOperatorMemoryManager CreateOperatorMemoryManager(string operatorName)
        {
            if (_managers.TryGetValue(operatorName, out var manager))
            {
                return manager;
            }
            manager = new OperatorMemoryManager(_streamName, operatorName, _meter);
            _managers.Add(operatorName, manager);
            return manager;
        }
    }
}
