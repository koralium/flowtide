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
    /// The log names an advance to C deletes, in the order it deletes them; reads nothing.
    /// </summary>
    internal static class LogTargets
    {
        /// <summary>
        /// Compactions starting at or below C, the highest start first; then each version below C from the highest: its commit, checksum, classic checkpoint and multipart parts in part order.
        /// </summary>
        public static IReadOnlyList<ClassifiedName> Order(LogClassification classes, long c)
        {
            var targets = new List<ClassifiedName>();
            foreach (var name in classes.Names)
            {
                if (Targeted(name, c))
                {
                    targets.Add(name);
                }
            }
            targets.Sort(Compare);
            return targets;
        }

        public static long Count(LogClassification classes, long c)
        {
            long count = 0;
            foreach (var name in classes.Names)
            {
                if (Targeted(name, c))
                {
                    count++;
                }
            }
            return count;
        }

        // Never version C, and never a hint, stage, temporary, control, protected, hidden, unknown or V2 name
        private static bool Targeted(in ClassifiedName name, long c) => name.Kind == LogNameKind.Compaction
            ? name.Version <= c
            : (name.Kind is LogNameKind.Commit or LogNameKind.Crc or LogNameKind.ClassicCheckpoint or LogNameKind.MultipartCheckpoint) && name.Version < c;

        // Compactions first, then the higher version, the higher end, the earlier kind and the name
        private static int Compare(ClassifiedName x, ClassifiedName y)
        {
            var compared = (y.Kind == LogNameKind.Compaction).CompareTo(x.Kind == LogNameKind.Compaction);
            if (compared == 0)
            {
                compared = y.Version.CompareTo(x.Version);
            }
            if (compared == 0)
            {
                compared = y.EndVersion.CompareTo(x.EndVersion);
            }
            if (compared == 0)
            {
                compared = Rank(x.Kind).CompareTo(Rank(y.Kind));
            }
            return compared != 0 ? compared : string.CompareOrdinal(x.Entry.Name, y.Entry.Name);
        }

        private static int Rank(LogNameKind kind) => kind switch
        {
            LogNameKind.Commit => 0,
            LogNameKind.Crc => 1,
            LogNameKind.ClassicCheckpoint => 2,
            _ => 3
        };
    }
}
