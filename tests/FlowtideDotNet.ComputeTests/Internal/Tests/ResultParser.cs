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

namespace FlowtideDotNet.ComputeTests.Internal.Tests
{
    internal static class ResultParser
    {
        public static ExpectedResult ParseExpectedResult(FuncTestCaseParser.ResultContext context)
        {

            var value = context.argument();
            if (value != null)
            {
                var dataValue = ArgumentParser.ParseArgument(value);
                return new ExpectedResult(dataValue, false);
            }

            return new ExpectedResult(null, true);
        }
    }
}
