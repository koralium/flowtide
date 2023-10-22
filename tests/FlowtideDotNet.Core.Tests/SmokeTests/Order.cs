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

using Newtonsoft.Json;

namespace FlowtideDotNet.Core.Tests.SmokeTests
{
    public class Order
    {
        public long Orderkey { get; set; }

        public long Custkey { get; set; }

        public string Orderstatus { get; set; }

        public double Totalprice { get; set; }

        public DateTime Orderdate { get; set; }

        public string Orderpriority { get; set; }

        public string Clerk { get; set; }

        public int Shippriority { get; set; }

        public string Comment { get; set; }

        public override bool Equals(object obj)
        {
            if (obj is Order o)
            {
                return Orderkey.Equals(o.Orderkey) &&
                    Custkey.Equals(o.Custkey) &&
                    Equals(Orderstatus, o.Orderstatus) &&
                    Totalprice.Equals(o.Totalprice) &&
                    Orderdate.Equals(o.Orderdate) &&
                    Equals(Orderpriority, o.Orderpriority) &&
                    Equals(Clerk, o.Clerk) &&
                    Shippriority.Equals(o.Shippriority) &&
                    Equals(Comment, o.Comment);
            }
            return false;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Orderkey, Custkey, Orderstatus, Totalprice, Orderdate, Orderpriority, Clerk, Shippriority);
        }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}
