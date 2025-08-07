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

namespace FlowtideDotNet.Substrait.FunctionExtensions
{
    public static class FunctionsHash
    {
        public const string Uri = "/functions_hash.yaml";

        /// <summary>
        /// Does a shake128 and returns a guid
        /// </summary>
        public const string XxHash128GuidString = "xxhash128_guid_string";
    }
}
