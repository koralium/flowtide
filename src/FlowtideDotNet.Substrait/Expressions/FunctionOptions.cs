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

namespace FlowtideDotNet.Substrait.Expressions
{
    /// <summary>
    /// Shared equality rules for function options, used by scalar, aggregate
    /// and window functions. Options change how a function behaves, so they are
    /// part of the function identity.
    /// </summary>
    internal static class FunctionOptions
    {
        /// <summary>
        /// No options and empty options mean the same thing to a consumer,
        /// the sql builder creates an empty list while serialization gives null.
        /// </summary>
        public static bool AreEqual(SortedList<string, string>? left, SortedList<string, string>? right)
        {
            if ((left?.Count ?? 0) != (right?.Count ?? 0))
            {
                return false;
            }
            if (left == null || right == null)
            {
                return true;
            }
            return left.SequenceEqual(right);
        }

        /// <summary>
        /// Adds the options to a hash code, null and empty give the same result.
        /// </summary>
        public static void AddToHash(ref HashCode code, SortedList<string, string>? options)
        {
            if (options == null)
            {
                return;
            }
            foreach (var option in options)
            {
                code.Add(option.Key);
                code.Add(option.Value);
            }
        }
    }
}
