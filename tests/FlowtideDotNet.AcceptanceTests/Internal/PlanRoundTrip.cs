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
using System.Collections;
using System.Reflection;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    /// <summary>
    /// Checks that a plan is the same after it is written and read back.
    /// </summary>
    public static class PlanRoundTrip
    {
        public static void AssertSurvives(Plan plan)
        {
            var json = SubstraitSerializer.SerializeToJson(plan);
            var back = SubstraitDeserializer.DeserializeFromJson(json);
            if (!plan.Equals(back))
            {
                var found = new List<string>();
                Collect(plan, back, "plan", 0, found);
                Assert.Fail("The plan is not the same after a round trip: " + string.Join("; ", found.Distinct().Take(6)));
            }
            // Equality does not look at everything that is written
            Assert.Equal(json, SubstraitSerializer.SerializeToJson(back));
        }

        // Lists every path where a value differs, equal parts are skipped
        private static void Collect(object? before, object? after, string path, int depth, List<string> found)
        {
            if (found.Count > 40 || depth > 80)
            {
                return;
            }
            if (before == null && after == null)
            {
                return;
            }
            if (before != null && after != null && before.Equals(after))
            {
                return;
            }
            if (before == null || after == null)
            {
                found.Add($"{path}: {Describe(before)} became {Describe(after)}");
                return;
            }
            var type = before.GetType();
            if (type != after.GetType())
            {
                found.Add($"{path}: {type.Name} became {after.GetType().Name}");
                return;
            }
            if (type.IsPrimitive || type.IsEnum || before is string || before is decimal)
            {
                found.Add($"{path}: {before} became {after}");
                return;
            }
            if (before is IDictionary beforeMap && after is IDictionary afterMap)
            {
                if (beforeMap.Count != afterMap.Count)
                {
                    found.Add($"{path}: {beforeMap.Count} entries became {afterMap.Count}");
                    return;
                }
                foreach (var key in beforeMap.Keys)
                {
                    Collect(beforeMap[key], afterMap.Contains(key) ? afterMap[key] : null, $"{path}[{key}]", depth + 1, found);
                }
                return;
            }
            if (before is IEnumerable beforeItems && after is IEnumerable afterItems)
            {
                var beforeList = beforeItems.Cast<object?>().ToList();
                var afterList = afterItems.Cast<object?>().ToList();
                if (beforeList.Count != afterList.Count)
                {
                    found.Add($"{path}: {beforeList.Count} items became {afterList.Count}");
                    return;
                }
                for (int i = 0; i < beforeList.Count; i++)
                {
                    Collect(beforeList[i], afterList[i], $"{path}[{i}]", depth + 1, found);
                }
                return;
            }
            int count = found.Count;
            foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                // Computed from the other properties
                if (property.GetIndexParameters().Length > 0 || property.Name == "OutputLength" || property.Name == "EmitSet")
                {
                    continue;
                }
                Collect(property.GetValue(before), property.GetValue(after), $"{type.Name}.{property.Name}", depth + 1, found);
            }
            if (found.Count == count)
            {
                found.Add($"{path}: a {type.Name} differs in a way its properties do not show");
            }
        }

        private static string Describe(object? value)
        {
            if (value == null)
            {
                return "nothing";
            }
            return value is ICollection collection ? $"{value.GetType().Name} of {collection.Count}" : value.GetType().Name;
        }
    }
}
