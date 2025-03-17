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

namespace FlowtideDotNet.Substrait.Conversion
{
    public static class SubstraitToDifferentialCompute
    {
        public static Plan Convert(Plan plan, bool addWriteRelation, string tableName, params string[] primaryKeys)
        {
            //if (addWriteRelation && primaryKeys.Length == 0)
            //{
            //    throw new InvalidOperationException("When adding a write relation a primary key must be set");
            //}
            SubstraitToDifferentialComputeVisitor visitor = new SubstraitToDifferentialComputeVisitor(addWriteRelation, tableName, primaryKeys.ToList());
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                plan.Relations[i] = visitor.Visit(plan.Relations[i], new object());
            }
            return plan;
        }
    }
}
