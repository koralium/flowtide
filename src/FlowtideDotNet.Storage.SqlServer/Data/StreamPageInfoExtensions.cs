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

namespace FlowtideDotNet.Storage.SqlServer.Data
{
    internal static class StreamPageInfoExtensions
    {
        public static void AddOrReplacePage(this Dictionary<long, ManagedStreamPage> dict, ManagedStreamPage page)
        {
            if (!dict.TryAdd(page.PageId, page))
            {
                dict[page.PageId] = page;
            }
        }

        public static void MarkPageDeleted(this Dictionary<long, ManagedStreamPage> dict, ManagedStreamPage page)
            => AddOrReplacePage(dict, page.CopyAsDeleted());

        public static void MarkPageDeleted(this Dictionary<long, ManagedStreamPage> dict, long key)
        {
            if (dict.TryGetValue(key, out var page))
            {
                AddOrReplacePage(dict, page.CopyAsDeleted());
            }
        }
    }
}
