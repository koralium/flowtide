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

using System.Collections.Immutable;

namespace FlowtideDotNet.Base
{
    internal class InitWatermarksEvent : ILockingEvent
    {
        public InitWatermarksEvent()
        {
            WatermarkNames = ImmutableHashSet<string>.Empty;
        }

        public InitWatermarksEvent(IReadOnlySet<string> watermarkNames)
        {
            WatermarkNames = watermarkNames;
        }

        public IReadOnlySet<string> WatermarkNames { get; }

        public InitWatermarksEvent AddWatermarkNames(IReadOnlySet<string> watermarkNames)
        {
            HashSet<string> newSet = new HashSet<string>(WatermarkNames);

            foreach (var name in watermarkNames)
            {
                newSet.Add(name);
            }

            return new InitWatermarksEvent(newSet);
        }
    }
}
