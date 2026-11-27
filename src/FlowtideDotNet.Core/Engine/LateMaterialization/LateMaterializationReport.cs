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
using System.Text;

namespace FlowtideDotNet.Core.Engine.LateMaterialization
{
    // What the planner found at one possible host
    internal sealed class RowStoreCandidate
    {
        public required Relation Host { get; init; }
        public required string Table { get; init; }
        // Output positions worth storing, in order
        public required List<int> Columns { get; init; }
        // Stored copies the reference passes, at most
        public required double Copies { get; init; }
        public required int Fetches { get; init; }
        // Bytes per row
        public required long Saving { get; init; }
        public required long Cost { get; init; }
        public long Net => Saving - Cost;
        public bool Admitted { get; set; }
        public string Reason { get; set; } = "";
    }

    // Columns that would need a store of their own
    internal sealed class MidPlanCandidate
    {
        public required string Producer { get; init; }
        public required int Columns { get; init; }
        public required long Saving { get; init; }
    }

    internal sealed class LateMaterializationReport
    {
        public List<RowStoreCandidate> Hosts { get; } = new List<RowStoreCandidate>();

        public List<MidPlanCandidate> MidPlan { get; } = new List<MidPlanCandidate>();

        // Why the whole plan was left alone, if it was
        public string Skipped { get; private set; } = "";

        // Leaves the whole plan alone, no host is admitted
        public void Skip(string reason)
        {
            Skipped = reason;
            foreach (var host in Hosts)
            {
                if (host.Admitted)
                {
                    host.Admitted = false;
                    host.Reason = reason;
                }
            }
        }

        public int Admitted => Hosts.Count(x => x.Admitted);

        // The stored positions of every admitted host
        public Dictionary<Relation, List<int>> Selection()
        {
            var selection = new Dictionary<Relation, List<int>>(ReferenceEqualityComparer.Instance);
            foreach (var host in Hosts)
            {
                if (host.Admitted)
                {
                    selection.Add(host.Host, host.Columns);
                }
            }
            return selection;
        }

        public override string ToString()
        {
            var text = new StringBuilder();
            if (Skipped.Length > 0)
            {
                text.Append("no row store placed, ").Append(Skipped);
            }
            else
            {
                text.Append(Admitted).Append(" of ").Append(Hosts.Count).Append(" row stores admitted");
            }
            foreach (var host in Hosts)
            {
                text.Append("; ").Append(host.Table).Append(": ").Append(host.Columns.Count).Append(" columns through ").Append(host.Copies)
                    .Append(" copies, net ").Append(host.Net).Append(" bytes per row, ").Append(host.Admitted ? "admitted" : "declined: " + host.Reason);
            }
            if (MidPlan.Count > 0)
            {
                text.Append("; ").Append(MidPlan.Count).Append(" places would need a store of their own");
            }
            return text.ToString();
        }
    }
}
