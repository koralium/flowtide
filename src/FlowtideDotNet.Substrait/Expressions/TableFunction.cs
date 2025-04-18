﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
    /// <summary>
    /// Represents a table function.
    /// A table function can only be used in the FROM clause or in joins.
    /// </summary>
    public sealed class TableFunction : IEquatable<TableFunction>
    {
        public required string ExtensionUri { get; set; }
        public required string ExtensionName { get; set; }

        public required List<Expression> Arguments { get; set; }

        public required NamedStruct TableSchema { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is TableFunction function &&
                   Equals(function);
        }

        public bool Equals(TableFunction? other)
        {
            return other != null &&
                   ExtensionUri == other.ExtensionUri &&
                   ExtensionName == other.ExtensionName &&
                   Arguments.SequenceEqual(other.Arguments) &&
                   Equals(TableSchema, other.TableSchema);
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
            code.Add(TableSchema);
            return code.ToHashCode();
        }

        public static bool operator ==(TableFunction? left, TableFunction? right)
        {
            return EqualityComparer<TableFunction>.Default.Equals(left, right);
        }

        public static bool operator !=(TableFunction? left, TableFunction? right)
        {
            return !(left == right);
        }
    }
}
