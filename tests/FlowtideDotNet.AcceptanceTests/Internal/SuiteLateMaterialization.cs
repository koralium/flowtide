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

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    public static class SuiteLateMaterialization
    {
        public const string Variable = "FLOWTIDE_TEST_LATE_MATERIALIZATION";

        // The environment sets the mode for the whole suite
        public static FlowtideBuilder WithSuiteLateMaterialization(this FlowtideBuilder builder)
        {
            var fromSuite = Environment.GetEnvironmentVariable(Variable);
            if (string.IsNullOrEmpty(fromSuite))
            {
                return builder;
            }
            // A misspelled mode would leave the suite on the default
            // Only a mode's own name, no number and no list of names
            if (!Enum.GetNames<LateMaterializationMode>().Contains(fromSuite, StringComparer.OrdinalIgnoreCase) ||
                !Enum.TryParse<LateMaterializationMode>(fromSuite, true, out var mode))
            {
                throw new ArgumentException($"'{fromSuite}' is no late materialization mode.");
            }
            return builder.SetLateMaterialization(mode);
        }
    }
}
