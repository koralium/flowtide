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

using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Storage.SqlServer.Data
{
    internal static class StreamPageInfoExtensions
    {
        public static bool TryGetValue(this HashSet<ManagedStreamPage> streamPages, long pageId, [NotNullWhen(true)] out ManagedStreamPage? info)
        {
            if (streamPages.Any(s => s.PageId == pageId))
            {
                info = streamPages.Last(s => s.PageId == pageId);
                return true;
            }

            info = null;
            return false;
        }

        public static void AddOrCreate(this Dictionary<long, HashSet<ManagedStreamPage>> dict, ManagedStreamPage page)
        {
            if (dict.TryGetValue(page.PageId, out var set))
            {
                set.Add(page);
            }
            else
            {
                dict.Add(page.PageId, [page]);
            }
        }

        /// <summary>
        /// Marks all versions in the set as deleted for the given <paramref name="page"/>.
        /// </summary>
        public static void MarkDeleted(this Dictionary<long, HashSet<ManagedStreamPage>> dict, ManagedStreamPage page)
            => MarkDeleted(dict, page.PageId);

        public static void MarkDeleted(this Dictionary<long, HashSet<ManagedStreamPage>> dict, long key)
        {
            if (dict.TryGetValue(key, out var set))
            {
                foreach (var item in set.ToArray())
                {
                    set.Remove(item);
                    set.Add(item.CopyAsDeleted());
                }
            }
        }

        public static bool TryGetLatestPageVersion(this Dictionary<long, HashSet<ManagedStreamPage>> dict, ManagedStreamPage page, [NotNullWhen(true)] out ManagedStreamPage? latestPage)
            => dict.TryGetLatestPageVersion(page.PageId, out latestPage);

        public static bool TryGetLatestPageVersion(this Dictionary<long, HashSet<ManagedStreamPage>> dict, long pageId, [NotNullWhen(true)] out ManagedStreamPage? latestPage)
        {
            if (dict.TryGetValue(pageId, out var set))
            {
                latestPage = set.MaxBy(s => s.Version);
                return true;
            }

            latestPage = null;
            return false;
        }

        public static IEnumerable<ManagedStreamPage> GetDeletedPageVersions(this Dictionary<long, HashSet<ManagedStreamPage>> dict)
        {
            return dict.Where(s => s.Value != null).SelectMany(s => s.Value).Where(s => s.ShouldDelete);
        }
    }
}
