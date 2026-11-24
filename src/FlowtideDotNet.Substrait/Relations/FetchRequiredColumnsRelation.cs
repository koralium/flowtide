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

namespace FlowtideDotNet.Substrait.Relations
{
    /// <summary>
    /// Columns to read from one row store through a reference column.
    /// </summary>
    public sealed class RowStoreFetch : IEquatable<RowStoreFetch>
    {
        /// <summary>
        /// Position of the reference column in the input.
        /// </summary>
        public required int ReferenceColumn { get; set; }

        /// <summary>
        /// Name of the store the references point into.
        /// </summary>
        public required string StoreName { get; set; }

        /// <summary>
        /// Positions in the store's list of stored columns.
        /// </summary>
        public required List<int> Columns { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is RowStoreFetch fetch && Equals(fetch);
        }

        public bool Equals(RowStoreFetch? other)
        {
            return other != null &&
                ReferenceColumn == other.ReferenceColumn &&
                StoreName == other.StoreName &&
                Columns.SequenceEqual(other.Columns);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(ReferenceColumn);
            code.Add(StoreName);
            foreach (var column in Columns)
            {
                code.Add(column);
            }
            return code.ToHashCode();
        }
    }

    /// <summary>
    /// Reads stored columns back through reference columns.
    /// The output is the input's columns, then each fetch's columns.
    /// A null reference gives nulls.
    /// </summary>
    public sealed class FetchRequiredColumnsRelation : Relation, IEquatable<FetchRequiredColumnsRelation>
    {
        public required Relation Input { get; set; }

        public required List<RowStoreFetch> Fetches { get; set; }

        public override int OutputLength
        {
            get
            {
                if (EmitSet)
                {
                    return Emit.Count;
                }
                int length = Input.OutputLength;
                foreach (var fetch in Fetches)
                {
                    length += fetch.Columns.Count;
                }
                return length;
            }
        }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitFetchRequiredColumnsRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is FetchRequiredColumnsRelation relation &&
                Equals(relation);
        }

        public bool Equals(FetchRequiredColumnsRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Fetches.SequenceEqual(other.Fetches) &&
                Equals(Input, other.Input);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            foreach (var fetch in Fetches)
            {
                code.Add(fetch);
            }
            code.Add(Input);
            return code.ToHashCode();
        }

        public static bool operator ==(FetchRequiredColumnsRelation? left, FetchRequiredColumnsRelation? right)
        {
            return EqualityComparer<FetchRequiredColumnsRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(FetchRequiredColumnsRelation? left, FetchRequiredColumnsRelation? right)
        {
            return !(left == right);
        }
    }
}
