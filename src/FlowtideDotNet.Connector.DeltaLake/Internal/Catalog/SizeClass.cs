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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using System.Numerics;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    // File size classes, in bits 2 to 5 of a row
    internal static class SizeClass
    {
        // Classes 0 to 14 are counted and 15 is never compacted
        public const int Counted = 15;
        public const byte Ineligible = 15;
        private const byte Largest = 14;
        // Class 1 starts at 8 KiB
        private const int BaseShift = 13;
        private const int FlagShift = 2;

        // Classes double from 8 KiB up to 64 MiB
        public static byte Of(DeltaFileRecord record)
        {
            if (!IsEligible(record))
            {
                return Ineligible;
            }
            return OfSize(record.Size);
        }

        public static byte OfSize(long size)
        {
            if (size < 0)
            {
                return Ineligible;
            }
            var units = (ulong)size >> BaseShift;
            if (units == 0)
            {
                return 0;
            }
            return (byte)Math.Min(Largest, BitOperations.Log2(units) + 1);
        }

        // The class as row flags, ORed into a row's first byte
        public static byte Flags(byte sizeClass)
        {
            return (byte)(sizeClass << FlagShift);
        }

        public static byte FromFlags(byte flags)
        {
            return (byte)((flags >> FlagShift) & Ineligible);
        }

        // Fields a deletion vector update never changes
        private static bool IsEligible(DeltaFileRecord record)
        {
            if (record.ClusteringProvider != null || record.BaseRowId != null || record.DefaultRowCommitVersion != null)
            {
                return false;
            }
            if (record.Tags != null && record.Tags.Count > 0)
            {
                return false;
            }
            if (record.PartitionValues != null && record.PartitionValues.Count > 0)
            {
                return false;
            }
            // Plain relative names only, nothing encoded
            var path = record.Path;
            return PathCanonicalizer.CheckRelative(path) == PathRejection.None
                && PathCanonicalizer.CheckUri(path) == PathRejection.None
                && PathCanonicalizer.SchemeLength(path) == 0
                && !path.Contains('%');
        }
    }
}
