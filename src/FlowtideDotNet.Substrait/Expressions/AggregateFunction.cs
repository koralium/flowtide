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


using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Substrait.Expressions
{
    public sealed class AggregateFunction : IEquatable<AggregateFunction>
    {
        public required string ExtensionUri { get; set; }
        public required string ExtensionName { get; set; }

        public required List<Expression> Arguments { get; set; }

        public SortedList<string, string>? Options { get; set; }

        /// <summary>
        /// The declared output (return) type of the aggregate, matching Substrait's
        /// AggregateFunction.output_type. Optional: front-ends that do not resolve it leave it null.
        /// Deliberately excluded from Equals/GetHashCode below - it is a derived annotation of
        /// (name, arguments), not part of the function's identity, so including it would needlessly
        /// perturb measure de-duplication.
        /// </summary>
        public SubstraitBaseType? OutputType { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is AggregateFunction function &&
                   Equals(function);
        }

        public bool Equals(AggregateFunction? other)
        {
            return other != null &&
                   ExtensionUri == other.ExtensionUri &&
                   ExtensionName == other.ExtensionName &&
                   Arguments.SequenceEqual(other.Arguments);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(ExtensionUri);
            code.Add(ExtensionName);
            foreach (var argument in Arguments)
            {
                code.Add(argument);
            }
            return code.ToHashCode();
        }

        public static bool operator ==(AggregateFunction? left, AggregateFunction? right)
        {
            return EqualityComparer<AggregateFunction>.Default.Equals(left, right);
        }

        public static bool operator !=(AggregateFunction? left, AggregateFunction? right)
        {
            return !(left == right);
        }
    }
}
