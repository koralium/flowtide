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

using FlowtideDotNet.Substrait;

namespace FlowtideDotNet.Core.Optimizer.WatermarkOutput
{
    internal static class WatermarkOutputOptimizer
    {
        public static Plan Optimize(Plan plan)
        {
            // Reference visitor figures out which relations where a watermark output can be pushed down into.
            WatermarkOutputReferenceVisitor referenceVisitor = new WatermarkOutputReferenceVisitor(plan);
            var relationMap = referenceVisitor.Run();

            // Cleanup visitor removes the temporary optimization property from the relations.
            WatermarkOutputCleanupVisitor cleanupVisitor = new WatermarkOutputCleanupVisitor();
            cleanupVisitor.Run(plan);

            // The main visitor applies the watermark output optimization based on the reference map.
            var visitor = new WatermarkOutputVisitor();
            visitor.Run(plan, relationMap);

            return plan;
        }
    }
}
