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

namespace FlowtideDotNet.Storage.DeviceFactories
{
    public class InMemoryDeviceFactory : INamedDeviceFactory
    {
        private class DeviceContainer
        {
            public DeviceContainer(FileDescriptor fileDescriptor, IDevice device)
            {
                FileDescriptor = fileDescriptor;
                Device = device;
            }

            public FileDescriptor FileDescriptor { get; }

            public IDevice Device { get; }
        }

        private readonly Dictionary<string, DeviceContainer> devices = new Dictionary<string, DeviceContainer>();
        private readonly object m_lock = new object();
        private readonly long deviceCapacity;
        private readonly long segmentSize;
        private readonly int parellalism;
        private readonly uint sectorSize;
        private readonly int latencyMs;

        public InMemoryDeviceFactory(long segmentSize = 1L << 25, int parellalism = 1, uint sectorSize = 512, int latencyMs = 0)
        {
            this.deviceCapacity = segmentSize * 64;
            this.segmentSize = segmentSize;
            this.parellalism = parellalism;
            this.sectorSize = sectorSize;
            this.latencyMs = latencyMs;
        }

        private string GetDeviceKey(FileDescriptor fileInfo)
        {
            return $"{fileInfo.directoryName}/{fileInfo.fileName}";
        }

        public void Delete(FileDescriptor fileInfo)
        {
            lock (m_lock) 
            {
                var key = GetDeviceKey(fileInfo);
                if (devices.TryGetValue(key, out var container))
                {
                    container.Device.Dispose();
                    devices.Remove(key);
                }
            }
        }

        public IDevice Get(FileDescriptor fileInfo)
        {
            lock (m_lock)
            {
                var key = GetDeviceKey(fileInfo);
                if (devices.TryGetValue(key, out var device))
                {
                    return device.Device;
                }
                else
                {
                    var memDevice = new LocalMemoryDevice(deviceCapacity, segmentSize, parellalism, latencyMs, sectorSize);
                    devices.Add(key, new DeviceContainer(fileInfo, memDevice));
                    return memDevice;
                }
            }
        }

        public void Initialize(string baseName)
        {
        }

        public IEnumerable<FileDescriptor> ListContents(string path)
        {
            lock(m_lock)
            {
                return devices.Where(x => x.Key.StartsWith(path)).Select(x => x.Value.FileDescriptor);
            }
        }
    }
}
