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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    internal class FileInformation
    {
        private object _lock = new object();
        public long FileId { get; }
        public int PageCount { get; }
        public int NonActivePageCount { get; private set; }
        public FileInformation(long fileId, int pageCount, int nonActivePageCount)
        {
            FileId = fileId;
            PageCount = pageCount;
            NonActivePageCount = nonActivePageCount;
        }

        public void AddNonActivePage()
        {
            lock (_lock)
            {
                NonActivePageCount++; 
            }
        }
    }
}
