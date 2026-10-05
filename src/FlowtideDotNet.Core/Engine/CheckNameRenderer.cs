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

namespace FlowtideDotNet.Core.Engine
{
    internal static class CheckNameRenderer
    {
        /// <summary>
        /// Replaces each {tag} placeholder in the check name with the tag's value.
        /// </summary>
        public static string Render(string checkName, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var rendered = checkName;
            for (int i = 0; i < tags.Length; i++)
            {
                rendered = rendered.Replace($"{{{tags[i].Key}}}", tags[i].Value?.ToString() ?? "null", StringComparison.OrdinalIgnoreCase);
            }
            return rendered;
        }
    }
}
