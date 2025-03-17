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

namespace FlowtideDotNet.AspNetCore.Internal.TimeSeries.Utils
{
    internal static class TagListUtils
    {
        public static int BinarySearchList(ReadOnlySpan<KeyValuePair<string, object?>> tags, List<KeyValuePair<string, string>[]> _tagsList)
        {
            int left = 0;
            int right = _tagsList.Count - 1;

            while (left <= right)
            {
                int mid = left + (right - left) / 2;

                if (tags.Length != _tagsList[mid].Length)
                {
                    if (tags.Length < _tagsList[mid].Length)
                    {
                        right = mid - 1;
                    }
                    else
                    {
                        left = mid + 1;
                    }

                    continue;
                }

                bool equal = true;
                for (int i = 0; i < tags.Length; i++)
                {
                    if (tags[i].Key != _tagsList[mid][i].Key)
                    {
                        equal = false;
                        if (tags[i].Key.CompareTo(_tagsList[mid][i].Key) < 0)
                        {
                            right = mid - 1;
                        }
                        else
                        {
                            left = mid + 1;
                        }
                        break;
                    }
                    if (tags[i].Value is string s && s != _tagsList[mid][i].Value)
                    {
                        equal = false;
                        if (s.CompareTo(_tagsList[mid][i].Value) < 0)
                        {
                            right = mid - 1;
                        }
                        else
                        {
                            left = mid + 1;
                        }
                        break;
                    }
                }

                if (equal)
                {
                    return mid;
                }
            }

            return ~left;
        }
    }
}
