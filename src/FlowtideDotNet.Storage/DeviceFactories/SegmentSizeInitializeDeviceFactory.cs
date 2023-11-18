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

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.DeviceFactories
{
    /// <summary>
    /// Helper device factory that initializes the device with a segment size.
    /// This can be useful to limit the writing size, such as with Azure blob storage.
    /// </summary>
    public class SegmentSizeInitializeDeviceFactory : INamedDeviceFactory
    {
        private readonly long segmentSize;
        private readonly INamedDeviceFactory inner;

        public SegmentSizeInitializeDeviceFactory(long segmentSize, INamedDeviceFactory inner)
        {
            this.segmentSize = segmentSize;
            this.inner = inner;
        }
        public void Delete(FileDescriptor fileInfo)
        {
            inner.Delete(fileInfo);
        }

        public IDevice Get(FileDescriptor fileInfo)
        {
            var device = inner.Get(fileInfo);
            device.Initialize(segmentSize);
            return device;
        }

        public void Initialize(string baseName)
        {
            inner.Initialize(baseName);
        }

        public IEnumerable<FileDescriptor> ListContents(string path)
        {
            return inner.ListContents(path);
        }
    }
}
