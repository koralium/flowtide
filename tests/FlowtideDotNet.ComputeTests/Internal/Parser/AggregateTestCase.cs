﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Core.ColumnStore;

namespace FlowtideDotNet.ComputeTests.Internal.Parser
{
    internal class AggregateTestCase
    {
        public string FunctionName { get; }

        public EventBatchData InputData { get; }

        public ExpectedResult ExpectedResult { get; }

        public SortedList<string, string> Options { get; }

        public AggregateTestCase(string functionName, EventBatchData inputData, ExpectedResult expectedResult, SortedList<string, string> options)
        {
            FunctionName = functionName;
            InputData = inputData;
            ExpectedResult = expectedResult;
            Options = options;

        }
    }
}
