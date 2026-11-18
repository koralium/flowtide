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

using Google.Protobuf;
using Protobuf = Substrait.Protobuf;

namespace FlowtideDotNet.Substrait
{
    /// <summary>
    /// Reaches the common part of any relation message, by its field name.
    /// </summary>
    internal static class RelCommonAccess
    {
        private const string CommonField = "common";

        private static IMessage? Inner(Protobuf.Rel rel)
        {
            var field = Protobuf.Rel.Descriptor.Oneofs[0].Accessor.GetCaseFieldDescriptor(rel);
            return field?.Accessor.GetValue(rel) as IMessage;
        }

        public static Protobuf.RelCommon? Get(Protobuf.Rel rel)
        {
            var inner = Inner(rel);
            var field = inner?.Descriptor.FindFieldByName(CommonField);
            return field?.Accessor.GetValue(inner) as Protobuf.RelCommon;
        }

        /// <summary>
        /// Null when this kind of relation message has no common part.
        /// </summary>
        public static Protobuf.RelCommon? GetOrCreate(Protobuf.Rel rel)
        {
            var inner = Inner(rel);
            var field = inner?.Descriptor.FindFieldByName(CommonField);
            if (field == null)
            {
                return null;
            }
            var common = field.Accessor.GetValue(inner) as Protobuf.RelCommon;
            if (common == null)
            {
                common = new Protobuf.RelCommon();
                field.Accessor.SetValue(inner, common);
            }
            return common;
        }
    }
}
