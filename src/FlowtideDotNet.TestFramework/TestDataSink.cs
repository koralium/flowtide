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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.TestFramework
{
    public class TestDataSink
    {
        private int updateVersion = 0;
        private int waitedVersion = 0;
        EventBatchData? _currentData;
        private object _lock = new object();
        public TestDataSink()
        {

        }

        public EventBatchData? CurrentData => _currentData;

        internal void OnDataUpdate(EventBatchData data)
        {
            lock (_lock)
            {
                updateVersion++;
                _currentData = data;
            }
        }

        public async Task<EventBatchData> WaitForUpdate()
        {
            Monitor.Enter(_lock);
            try
            {
                while (updateVersion == waitedVersion)
                {
                    Monitor.Exit(_lock);
                    await Task.Delay(100);
                    Monitor.Enter(_lock);
                }
                waitedVersion = updateVersion;
                if (_currentData == null)
                {
                    throw new InvalidOperationException("No data available");
                }
                return _currentData;
            }
            finally
            {
                if (Monitor.IsEntered(_lock))
                {
                    Monitor.Exit(_lock);
                }
            }
        }

        public bool IsCurrentDataEqual<T>(IEnumerable<T> data)
        {
            var expectedBatch = BatchConverter.ConvertToBatchSorted(data, GlobalMemoryManager.Instance);
            return new EventBatchDataComparer().Equals(expectedBatch, _currentData);
        }
    }
}
