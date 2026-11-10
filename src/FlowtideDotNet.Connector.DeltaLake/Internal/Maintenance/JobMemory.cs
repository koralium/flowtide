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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// The managed and native memory ledgers of one job, used from one thread at a time.
    /// </summary>
    internal sealed class JobMemory
    {
        public const long DefaultManagedCap = 84L << 20;
        public const long DefaultNativeCap = 256L << 20;

        private long _managed;
        private long _native;

        public JobMemory(long managedCap = DefaultManagedCap, long nativeCap = DefaultNativeCap)
        {
            ManagedCap = managedCap;
            NativeCap = nativeCap;
        }

        public long ManagedCap { get; }

        public long NativeCap { get; }

        public long Managed => _managed;

        public long ManagedPeak { get; private set; }

        public long Native => _native;

        public long NativePeak { get; private set; }

        // Native readers this job opened, so tests can tell a scan refused a file before any native open
        public long NativeOpens { get; private set; }

        internal void CountNativeOpen() => NativeOpens++;

        // Runs before every managed reservation, tests throw from it to inject an allocation failure
        public Action<long>? BeforeReserveForTests { get; set; }

        public void Reserve(long bytes)
        {
            BeforeReserveForTests?.Invoke(bytes);
            if (bytes < 0 || bytes > ManagedCap - _managed)
            {
                throw new CheckpointDeclinedException(MaintenanceReason.ManagedMemoryCap, DeclineDependency.None, $"{bytes} bytes over the {ManagedCap} byte managed cap with {_managed} reserved");
            }
            _managed += bytes;
            ManagedPeak = Math.Max(ManagedPeak, _managed);
        }

        // False instead of a decline when over the cap
        public bool TryReserve(long bytes)
        {
            BeforeReserveForTests?.Invoke(bytes);
            if (bytes < 0 || bytes > ManagedCap - _managed)
            {
                return false;
            }
            _managed += bytes;
            ManagedPeak = Math.Max(ManagedPeak, _managed);
            return true;
        }

        public void Release(long bytes)
        {
            if (bytes < 0 || bytes > _managed)
            {
                throw new InvalidOperationException("Released more managed memory than was reserved");
            }
            _managed -= bytes;
        }

        // Buffers are grow-only and few per job, so they are allocated exactly instead of pooled
        public byte[] Allocate(int length)
        {
            Reserve(length);
            return GC.AllocateUninitializedArray<byte>(length);
        }

        public void Free(byte[] array)
        {
            Release(array.Length);
        }

        public NativeReservation ReserveNative(long bytes)
        {
            if (bytes < 0 || bytes > NativeCap - _native)
            {
                throw new CheckpointDeclinedException(MaintenanceReason.NativeMemoryCap, DeclineDependency.None, $"{bytes} bytes over the {NativeCap} byte native cap with {_native} reserved");
            }
            _native += bytes;
            NativePeak = Math.Max(NativePeak, _native);
            return new NativeReservation(this, bytes);
        }

        // False instead of a decline when over the cap
        public bool TryReserveNative(long bytes)
        {
            if (bytes < 0 || bytes > NativeCap - _native)
            {
                return false;
            }
            _native += bytes;
            NativePeak = Math.Max(NativePeak, _native);
            return true;
        }

        internal void ReleaseNative(long bytes)
        {
            if (bytes < 0 || bytes > _native)
            {
                throw new InvalidOperationException("Released more native memory than was reserved");
            }
            _native -= bytes;
        }
    }

    /// <summary>
    /// A native term held while its object is open; disposing the object releases it.
    /// </summary>
    internal sealed class NativeReservation : IDisposable
    {
        private JobMemory? _memory;

        public NativeReservation(JobMemory memory, long bytes)
        {
            _memory = memory;
            Bytes = bytes;
        }

        public long Bytes { get; }

        public void Dispose()
        {
            Interlocked.Exchange(ref _memory, null)?.ReleaseNative(Bytes);
        }
    }
}
