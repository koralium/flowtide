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

using FlowtideDotNet.Storage.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.LocalCache
{
    internal class LocalCacheMetricValues
    {
        private long _persistentWriteCount;
        private long _persistentReadCount;
        private long _persistentDeleteCount;
        private Histogram<long>? _persistentBytesRead;
        private Histogram<long>? _persistentBytesWritten;
        private TagList _tagList;

        public long PersistentWriteCount => Interlocked.Read(ref _persistentWriteCount);

        public long PersistentReadCount => Interlocked.Read(ref _persistentReadCount);

        public long PersistentDeleteCount => Interlocked.Read(ref _persistentDeleteCount);

        public TagList TagList => _tagList;

        public void AddPersistentRead()
        {
            Interlocked.Increment(ref _persistentReadCount);
        }

        public void AddPersistentWrite()
        {
            Interlocked.Increment(ref _persistentWriteCount);
        }

        public void AddPersistentDelete()
        {
            Interlocked.Increment(ref _persistentDeleteCount);
        }

        public void SetPersistentBytesReadHistogram(Histogram<long> histogram)
        {
            _persistentBytesRead = histogram;
        }

        public void SetPersistentBytesWrittenHistogram(Histogram<long> histogram)
        {
            _persistentBytesWritten = histogram;
        }

        public void SetTagList(TagList tagList)
        {
            _tagList = tagList;
        }

        public void AddPersistentBytesWritten(long writeBytes)
        {
            if (_persistentBytesWritten != null)
            {
                _persistentBytesWritten.Record(writeBytes, _tagList);
            }
        }

        public void AddPersistentBytesRead(long readBytes) 
        { 
            if (_persistentBytesRead != null)
            {
                _persistentBytesRead.Record(readBytes, _tagList);
            }
        }
    }
}
