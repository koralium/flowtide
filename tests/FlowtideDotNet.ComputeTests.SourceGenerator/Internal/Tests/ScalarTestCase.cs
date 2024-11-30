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

using System.Collections.Generic;

namespace FlowtideDotNet.ComputeTests.Internal.Tests
{
    internal class ScalarTestCase
    {
        public string FunctionName { get; }
        public IReadOnlyList<string> Arguments { get; }

        public ExpectedResult ExpectedResult { get; }

        public SortedList<string, string> Options { get; }

        public ScalarTestCase(string functionName, IReadOnlyList<string> arguments, ExpectedResult expectedResult, SortedList<string, string> options)
        {
            FunctionName = functionName;
            Arguments = arguments;
            ExpectedResult = expectedResult;
            Options = options;
        }
    }
}
