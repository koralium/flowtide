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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.TreeStorage
{
    internal static class IntListSearch
    {
        public static (int, int) SearchBoundries(in List<int> list, in int value)
        {
            if (list.Count == 0)
            {
                return (-1, -1);
            }

            if (list[0] > value)
            {
                return (-1, -1);
            }

            if (list[list.Count - 1] < value)
            {
                return (-1, -1);
            }

            var left = 0;
            var right = list.Count - 1;

            while (left < right)
            {
                var mid = left + (right - left) / 2;

                if (list[mid] < value)
                {
                    left = mid + 1;
                }
                else
                {
                    right = mid;
                }
            }

            if (list[left] != value)
            {
                return (-1, -1);
            }

            var start = left;

            left = 0;
            right = list.Count - 1;

            while (left < right)
            {
                var mid = left + (right - left + 1) / 2;

                if (list[mid] > value)
                {
                    right = mid - 1;
                }
                else
                {
                    left = mid;
                }
            }

            return (start, left);

        }
    }
}
