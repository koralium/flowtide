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

using System.Text;

namespace FlowtideDotNet.Base.Metrics.Extensions
{
    internal static class TagsExtensions
    {
        public static string ConvertToString(this ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            if (tags.Length == 0)
            {
                return string.Empty;
            }
            StringBuilder stringBuilder = new StringBuilder();
            var lengthMinusOne = tags.Length - 1;
            for (int i = 0; i < lengthMinusOne; i++)
            {
                var tag = tags[i];
                stringBuilder.Append(tag.Key);
                stringBuilder.Append('=');
                stringBuilder.Append(tag.Value);
                stringBuilder.Append(',');
            }
            stringBuilder.Append(tags[lengthMinusOne].Key);
            stringBuilder.Append(tags[lengthMinusOne].Value);
            return stringBuilder.ToString();
        }
    }
}
