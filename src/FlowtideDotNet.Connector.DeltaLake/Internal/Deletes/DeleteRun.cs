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

using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Deletes
{
    // One commit's touched files, their vectors and claims
    internal sealed class DeleteRun
    {
        private readonly IFileStorage _storage;
        private readonly IOPath _table;
        // Read on a file's first hit and kept for the commit
        private readonly Dictionary<int, IDeleteVector> _existing = new Dictionary<int, IDeleteVector>();
        private long _claimFreeBytes;

        public DeleteRun(IFileStorage storage, IOPath table)
        {
            _storage = storage;
            _table = table;
        }

        public Dictionary<int, DeltaFileRecord> Records { get; } = new Dictionary<int, DeltaFileRecord>();

        public Dictionary<int, long> FooterRows { get; } = new Dictionary<int, long>();

        // Only files with a new claim, the output writes these
        public Dictionary<int, ModifiableDeleteVector> Claims { get; } = new Dictionary<int, ModifiableDeleteVector>();

        public DeleteStatistics Statistics { get; } = new DeleteStatistics();

        // Vector bytes of files read without a claim
        public long ClaimFreeVectorBytes => _claimFreeBytes;

        public async ValueTask Load(int id)
        {
            if (_existing.ContainsKey(id))
            {
                return;
            }
            var vector = Records[id].DeletionVector;
            if (vector == null)
            {
                _existing.Add(id, EmptyDeleteVector.Instance);
                return;
            }
            _existing.Add(id, await DeletionVectorReader.ReadDeletionVector(_storage, _table, vector));
            Statistics.VectorsRead++;
            _claimFreeBytes += vector.SizeInBytes;
            Statistics.PeakClaimFreeVectorBytes = Math.Max(Statistics.PeakClaimFreeVectorBytes, _claimFreeBytes);
        }

        // In the existing vector or claimed this commit
        public bool IsDeleted(int id, long position)
        {
            return Claims.TryGetValue(id, out var claimed) ? claimed.Contains(position) : _existing[id].Contains(position);
        }

        public void Claim(int id, long position)
        {
            if (!Claims.TryGetValue(id, out var claimed))
            {
                claimed = new ModifiableDeleteVector(_existing[id]);
                Claims.Add(id, claimed);
                _claimFreeBytes -= Records[id].DeletionVector?.SizeInBytes ?? 0;
            }
            claimed.Add(position);
            Statistics.Claims++;
        }
    }

    // Counts and ticks of one commit's matching
    internal sealed class DeleteStatistics
    {
        public int Passes { get; set; }

        public long FilesScanned { get; set; }

        public long RowsScanned { get; set; }

        public long RowsHashed { get; set; }

        // No stored row can equal them
        public long RowsSkipped { get; set; }

        public long Hits { get; set; }

        public long Claims { get; set; }

        public long VectorsRead { get; set; }

        // The highest total, decides whether to drop them
        public long PeakClaimFreeVectorBytes { get; set; }

        public long RowGroupsRawRead { get; set; }

        public long RawLevelsRead { get; set; }

        public long OpenTicks { get; set; }

        // Reading and decoding batches
        public long ReadTicks { get; set; }

        public long PrecisionTicks { get; set; }

        public long HashTicks { get; set; }
    }
}
