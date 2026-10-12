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

namespace FlowtideDotNet.Core.Operators.Check
{
    /// <summary>
    /// Running counts of one check, kept by the weight mutator.
    /// </summary>
    internal sealed class CheckCounts
    {
        /// <summary>
        /// Issue keys with a positive weight.
        /// </summary>
        public long ActiveIssues;

        /// <summary>
        /// Sum of the positive issue key weights.
        /// </summary>
        public long FailingRows;
    }
}
