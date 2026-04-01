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

using System.Text.Json.Serialization;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal enum LineageTransformationType
    {
        Direct,
        Indirect
    }

    internal enum LineageTransformationSubtype
    {
        // Direct
        Identity,
        Transformation,
        Aggregation,

        // Indirect
        Join,
        GroupBy,
        Filter,
        Sort,
        Window,
        Conditional
    }

    internal class LineageTransformation : IEquatable<LineageTransformation>
    {
        [JsonPropertyName("type")]
        public LineageTransformationType Type { get; }

        [JsonPropertyName("subtype")]
        public LineageTransformationSubtype SubType { get; }

        [JsonPropertyName("description")]
        public string Description { get; }

        [JsonPropertyName("masking")]
        public bool Masking { get; }

        public LineageTransformation(LineageTransformationType type, LineageTransformationSubtype subType, string description = "", bool masking = false)
        {
            Type = type;
            SubType = subType;
            Description = description;
            Masking = masking;
        }

        public bool Equals(LineageTransformation? other)
        {
            return other != null
                && Type == other.Type
                && SubType == other.SubType
                && Description == other.Description
                && Masking == other.Masking;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as LineageTransformation);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Type, SubType, Description, Masking);
        }
    }
}
