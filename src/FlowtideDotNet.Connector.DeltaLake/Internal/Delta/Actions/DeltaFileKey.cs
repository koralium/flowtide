using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions
{
    internal struct DeltaFileKey : IEquatable<DeltaFileKey>
    {
        public string Path { get; }
        public string? DeletionVectorId { get; }

        public DeltaFileKey(string path, string? deletionVectorId)
        {
            Path = path;
            DeletionVectorId = deletionVectorId;
        }

        public bool Equals(DeltaFileKey other)
        {
            return Path == other.Path && DeletionVectorId == other.DeletionVectorId;
        }

        public override bool Equals([NotNullWhen(true)] object? obj)
        {
            if (obj is DeltaFileKey other)
            {
                return Equals(other);
            }
            return false;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Path, DeletionVectorId);
        }
    }
}
