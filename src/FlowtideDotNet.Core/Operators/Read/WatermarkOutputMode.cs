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
using System.Reflection;
using System.Runtime.Serialization;

namespace FlowtideDotNet.Core.Operators.Read
{
    internal enum WatermarkOutputMode
    {
        [EnumMember(Value = "AFTER_ALL_DATA")]
        AfterAllData = 0, // Watermark is emitted when all available data is sent
        [EnumMember(Value = "ON_EACH_BATCH")]
        OnEachBatch = 1, // Watermark is emitted after every batch of data is sent
    }

    internal static class WatermarkOutputModeHelper
    {
        public static bool TryParseWatermarkOutputMode(string value, out WatermarkOutputMode result)
        {
            var type = typeof(WatermarkOutputMode);

            foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Static))
            {
                var enumMemberAttr = field.GetCustomAttribute<EnumMemberAttribute>();
                if (enumMemberAttr != null && enumMemberAttr.Value != null && enumMemberAttr.Value.Equals(value, StringComparison.OrdinalIgnoreCase))
                {
                    result = (WatermarkOutputMode)field.GetValue(null)!;
                    return true;
                }
            }

            // Fallback: Try regular name-based parsing
            return Enum.TryParse(value, ignoreCase: true, out result);
        }
    }
}
