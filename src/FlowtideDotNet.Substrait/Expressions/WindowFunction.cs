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

using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Expressions
{
    public class WindowFunction : IEquatable<WindowFunction>
    {
        public required string ExtensionUri { get; set; }
        public required string ExtensionName { get; set; }

        public required List<Expression> Arguments { get; set; }

        public SortedList<string, string>? Options { get; set; }

        public WindowBound? LowerBound { get; set; }

        public WindowBound? UpperBound { get; set; }

        public bool Equals(WindowFunction? other)
        {
            if (ReferenceEquals(null, other))
                return false;

            if (ReferenceEquals(this, other))
                return true;

            if (!ExtensionUri.Equals(other.ExtensionUri))
            {
                return false;
            }

            if (!ExtensionName.Equals(other.ExtensionName))
            {
                return false;
            }

            if (!Arguments.SequenceEqual(other.Arguments))
            {
                return false;
            }

            if (!FunctionOptions.AreEqual(Options, other.Options))
            {
                return false;
            }

            if (LowerBound != null && other.LowerBound != null)
            {
                if (!LowerBound.Equals(other.LowerBound))
                {
                    return false;
                }
            }
            else if (LowerBound != other.LowerBound)
            {
                return false;
            }

            if (UpperBound != null && other.UpperBound != null)
            {
                if (!UpperBound.Equals(other.UpperBound))
                {
                    return false;
                }
            }
            else if (UpperBound != other.UpperBound)
            {
                return false;
            }

            return true;
        }

        public override bool Equals(object? obj)
        {
            return obj is WindowFunction other &&
                Equals(other);
        }

        public override int GetHashCode()
        {
            HashCode code = new HashCode();
            code.Add(ExtensionUri);
            code.Add(ExtensionName);
            foreach (var argument in Arguments)
            {
                code.Add(argument);
            }
            FunctionOptions.AddToHash(ref code, Options);
            if (LowerBound != null)
            {
                code.Add(LowerBound);
            }
            if (UpperBound != null)
            {
                code.Add(UpperBound);
            }
            return code.ToHashCode();
        }

        public static bool operator ==(WindowFunction? left, WindowFunction? right)
        {
            return EqualityComparer<WindowFunction>.Default.Equals(left, right);
        }

        public static bool operator !=(WindowFunction? left, WindowFunction? right)
        {
            return !(left == right);
        }
    }

    public enum WindowBoundType
    {
        PreceedingRow,
        FollowingRow,
        CurrentRow,
        Unbounded,
        PreceedingRange,
        FollowingRange
    }

    public abstract class WindowBound
    {
        public abstract WindowBoundType Type { get; }
    }

    public class PreceedingRowWindowBound : WindowBound, IEquatable<PreceedingRowWindowBound>
    {
        public long Offset { get; set; }

        public override WindowBoundType Type => WindowBoundType.PreceedingRow;

        public bool Equals(PreceedingRowWindowBound? other)
        {
            return other != null &&
                Offset.Equals(other.Offset);
        }

        public override bool Equals(object? obj)
        {
            return obj is PreceedingRowWindowBound bounds &&
                Equals(bounds);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(Type);
            code.Add(Offset);
            return code.ToHashCode();
        }

        public static bool operator ==(PreceedingRowWindowBound? left, PreceedingRowWindowBound? right)
        {
            return EqualityComparer<PreceedingRowWindowBound>.Default.Equals(left, right);
        }

        public static bool operator !=(PreceedingRowWindowBound? left, PreceedingRowWindowBound? right)
        {
            return !(left == right);
        }
    }

    public class PreceedingRangeWindowBound : WindowBound, IEquatable<PreceedingRangeWindowBound>
    {
        public required Expression Expression { get; set; }

        public override WindowBoundType Type => WindowBoundType.PreceedingRange;

        public bool Equals(PreceedingRangeWindowBound? other)
        {
            return other != null &&
                Expression.Equals(other.Expression);
        }

        public override bool Equals(object? obj)
        {
            return obj is PreceedingRangeWindowBound bounds &&
                Equals(bounds);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(Type);
            code.Add(Expression);
            return code.ToHashCode();
        }

        public static bool operator ==(PreceedingRangeWindowBound? left, PreceedingRangeWindowBound? right)
        {
            return EqualityComparer<PreceedingRangeWindowBound>.Default.Equals(left, right);
        }

        public static bool operator !=(PreceedingRangeWindowBound? left, PreceedingRangeWindowBound? right)
        {
            return !(left == right);
        }
    }

    public class FollowingRowWindowBound : WindowBound, IEquatable<FollowingRowWindowBound>
    {
        public long Offset { get; set; }

        public override WindowBoundType Type => WindowBoundType.FollowingRow;

        public bool Equals(FollowingRowWindowBound? other)
        {
            return other != null &&
                Offset.Equals(other.Offset);
        }

        public override bool Equals(object? obj)
        {
            return obj is FollowingRowWindowBound bounds &&
                Equals(bounds);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(Type);
            code.Add(Offset);
            return code.ToHashCode();
        }

        public static bool operator ==(FollowingRowWindowBound? left, FollowingRowWindowBound? right)
        {
            return EqualityComparer<FollowingRowWindowBound>.Default.Equals(left, right);
        }

        public static bool operator !=(FollowingRowWindowBound? left, FollowingRowWindowBound? right)
        {
            return !(left == right);
        }
    }

    public class FollowingRangeWindowBound : WindowBound, IEquatable<FollowingRangeWindowBound>
    {
        public required Expression Expression { get; set; }

        public override WindowBoundType Type => WindowBoundType.FollowingRange;

        public bool Equals(FollowingRangeWindowBound? other)
        {
            return other != null &&
                Expression.Equals(other.Expression);
        }

        public override bool Equals(object? obj)
        {
            return obj is FollowingRangeWindowBound bounds &&
                Equals(bounds);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(Type);
            code.Add(Expression);
            return code.ToHashCode();
        }

        public static bool operator ==(FollowingRangeWindowBound? left, FollowingRangeWindowBound? right)
        {
            return EqualityComparer<FollowingRangeWindowBound>.Default.Equals(left, right);
        }

        public static bool operator !=(FollowingRangeWindowBound? left, FollowingRangeWindowBound? right)
        {
            return !(left == right);
        }
    }

    public class CurrentRowWindowBound : WindowBound, IEquatable<CurrentRowWindowBound>
    {
        public override WindowBoundType Type => WindowBoundType.CurrentRow;

        public bool Equals(CurrentRowWindowBound? other)
        {
            return other != null;
        }

        public override bool Equals(object? obj)
        {
            return obj is CurrentRowWindowBound bounds &&
                Equals(bounds);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(Type);
            return code.ToHashCode();
        }

        public static bool operator ==(CurrentRowWindowBound? left, CurrentRowWindowBound? right)
        {
            return EqualityComparer<CurrentRowWindowBound>.Default.Equals(left, right);
        }

        public static bool operator !=(CurrentRowWindowBound? left, CurrentRowWindowBound? right)
        {
            return !(left == right);
        }
    }

    public class UnboundedWindowBound : WindowBound, IEquatable<UnboundedWindowBound>
    {
        public override WindowBoundType Type => WindowBoundType.Unbounded;

        public bool Equals(UnboundedWindowBound? other)
        {
            return other != null;
        }

        public override bool Equals(object? obj)
        {
            return obj is UnboundedWindowBound bounds &&
                Equals(bounds);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(Type);
            return code.ToHashCode();
        }

        public static bool operator ==(UnboundedWindowBound? left, UnboundedWindowBound? right)
        {
            return EqualityComparer<UnboundedWindowBound>.Default.Equals(left, right);
        }

        public static bool operator !=(UnboundedWindowBound? left, UnboundedWindowBound? right)
        {
            return !(left == right);
        }
    }
}
