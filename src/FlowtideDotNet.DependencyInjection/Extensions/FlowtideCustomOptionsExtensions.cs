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

using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Engine.FailureStrategies;

namespace FlowtideDotNet.DependencyInjection
{
    public static class FlowtideCustomOptionsExtensions
    {
        /// <summary>
        /// When a failure occurs call exit on the current process.
        /// </summary>
        /// <param name="builder"></param>
        public static void WithExitProcessOnFailure(this FlowtideBuilder builder)
        {
            builder.WithFailureListener<ExitProcessStrategy>();
        }
    }
}
