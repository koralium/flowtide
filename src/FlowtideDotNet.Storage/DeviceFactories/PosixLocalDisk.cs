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

using FlowtideDotNet.Storage.FileCache.Internal;
using FASTER.core;
using Microsoft.Win32.SafeHandles;
using System.Collections.Concurrent;
using System.Reflection;

namespace FlowtideDotNet.Storage.DeviceFactories
{
    internal class PosixLocalDisk : IDevice
    {
        private readonly IDevice device;
        private ManagedLocalStorageDevice _managedLocmalStorageDevice;

        private delegate string GetSegmentNameDelegate(int segmentId);
        private GetSegmentNameDelegate _getSegmentName;
        private ConcurrentDictionary<int, SafeFileHandle> _segmentToHandle = new ConcurrentDictionary<int, SafeFileHandle>();

        public PosixLocalDisk(IDevice device)
        {
            if (device is ManagedLocalStorageDevice managedLocmalStorageDevice)
            {
                _managedLocmalStorageDevice = managedLocmalStorageDevice;
                var method = typeof(ManagedLocalStorageDevice).GetMethod("GetSegmentName", BindingFlags.Instance | BindingFlags.NonPublic);
                _getSegmentName = method.CreateDelegate<GetSegmentNameDelegate>(_managedLocmalStorageDevice);
            }
            this.device = device;
        }

        public uint SectorSize => device.SectorSize;

        public string FileName => device.FileName;

        public long Capacity => device.Capacity;

        public long SegmentSize => device.SegmentSize;

        public int StartSegment => device.StartSegment;

        public int EndSegment => device.EndSegment;

        public int ThrottleLimit { get => device.ThrottleLimit; set { device.ThrottleLimit = value; } }

        public void Dispose()
        {
            device.Dispose();
        }

        public long GetFileSize(int segment)
        {
            return device.GetFileSize(segment);
        }

        public void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            device.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        public void ReadAsync(int segmentId, ulong sourceAddress, nint destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context)
        {
            device.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
        }

        public void ReadAsync(ulong alignedSourceAddress, nint alignedDestinationAddress, uint aligned_read_length, DeviceIOCompletionCallback callback, object context)
        {
            device.ReadAsync(alignedSourceAddress, alignedDestinationAddress, aligned_read_length, callback, context);
        }

        public void RemoveSegment(int segment)
        {
            device.RemoveSegment(segment);
        }

        public void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            device.RemoveSegmentAsync(segment, callback, result);
        }

        public void Reset()
        {
            device.Reset();
        }

        public bool Throttle()
        {
            return device.Throttle();
        }

        public void TruncateUntilAddress(long toAddress)
        {
            device.TruncateUntilAddress(toAddress);
        }

        public void TruncateUntilAddressAsync(long toAddress, AsyncCallback callback, IAsyncResult result)
        {
            device.TruncateUntilAddressAsync(toAddress, callback, result);
        }

        public void TruncateUntilSegment(int toSegment)
        {
            device.TruncateUntilSegment(toSegment);
        }

        public void TruncateUntilSegmentAsync(int toSegment, AsyncCallback callback, IAsyncResult result)
        {
            device.TruncateUntilSegmentAsync(toSegment, callback, result);
        }

        public bool TryComplete()
        {
            return device.TryComplete();
        }

        public void WriteAsync(nint sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            device.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, (uint errorCode, uint numBytes, object context) =>
            {
                var handle = _segmentToHandle.GetOrAdd(segmentId, (segmentId) =>
                {
                    var filePath = _getSegmentName(segmentId);
                    return File.OpenHandle(filePath, share: FileShare.ReadWrite);
                });

                PosixUnix.SetAdvice(handle, 0, 0, PosixUnix.FileAdvice.POSIX_FADV_DONTNEED);

                callback(errorCode, numBytes, context);
            }, context);
        }

        public void WriteAsync(nint alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            device.WriteAsync(alignedSourceAddress, alignedDestinationAddress, numBytesToWrite, callback, context);
        }
    }
}
