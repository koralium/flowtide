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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// A started advance's planned deletion paths that a foreign change referenced again, which its payload phase passes without deleting.
    /// </summary>
    internal sealed class ExclusionSet
    {
        // A string's header and terminator with the set's entry and bucket slots while they double
        private const int EntryOverhead = 96;

        private readonly HashSet<string> _paths;

        private ExclusionSet(HashSet<string> paths, long bytes)
        {
            _paths = paths;
            Bytes = bytes;
        }

        public static ExclusionSet Empty { get; } = new ExclusionSet(new HashSet<string>(StringComparer.Ordinal), 0);

        // Takes an ordinal set of exact deletion paths and the managed bytes they take
        public static ExclusionSet Of(HashSet<string> paths, long bytes) => paths.Count == 0 ? Empty : new ExclusionSet(paths, bytes);

        public int Count => _paths.Count;

        // Charged by each turn that holds the set
        public long Bytes { get; }

        // The managed bytes a path takes in a set
        public static long BytesOf(string path) => EntryOverhead + 2L * path.Length;

        // A part's exact deletion path
        public bool Contains(string partDeletionPath) => _paths.Contains(partDeletionPath);
    }
}
