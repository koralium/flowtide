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
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Expressions
{
    public class WindowFunction
    {
        public required string ExtensionUri { get; set; }
        public required string ExtensionName { get; set; }

        public required List<Expression> Arguments { get; set; }

        public SortedList<string, string>? Options { get; set; }

        public WindowBound? LowerBound { get; set; }

        public WindowBound? UpperBound { get; set; }
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

    public class PreceedingRowWindowBound : WindowBound
    {
        public long Offset { get; set; }

        public override WindowBoundType Type => WindowBoundType.PreceedingRow;
    }

    public class PreceedingRangeWindowBound : WindowBound
    {
        public required Expression Expression { get; set; }

        public override WindowBoundType Type => WindowBoundType.PreceedingRange;
    }

    public class FollowingRowWindowBound : WindowBound
    {
        public long Offset { get; set; }

        public override WindowBoundType Type => WindowBoundType.FollowingRow;
    }

    public class FollowingRangeWindowBound : WindowBound
    {
        public required Expression Expression { get; set; }

        public override WindowBoundType Type => WindowBoundType.FollowingRange;
    }

    public class CurrentRowWindowBound : WindowBound
    {
        public override WindowBoundType Type => WindowBoundType.CurrentRow;
    }

    public class UnboundedWindowBound : WindowBound
    {
        public override WindowBoundType Type => WindowBoundType.Unbounded;
    }
}
