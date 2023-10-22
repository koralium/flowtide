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

namespace FlowtideDotNet.Core.Tests.SmokeTests
{
    public class LineItem
    {
        public long Orderkey { get; set; }

        public long Partkey { get; set; }

        public long Suppkey { get; set; }

        public int Linenumber { get; set; }

        public double Quantity { get; set; }

        public double Extendedprice { get; set; }

        public double Discount { get; set; }

        public double Tax { get; set; }

        public string Returnflag { get; set; }

        public string Linestatus { get; set; }

        public DateTime Shipdate { get; set; }

        public DateTime Commitdate { get; set; }

        public DateTime Receiptdate { get; set; }

        public string Shipinstruct { get; set; }

        public string Shipmode { get; set; }

        public string Comment { get; set; }

        public override bool Equals(object obj)
        {
            if (obj is LineItem l)
            {
                return l.Comment == Comment &&
                    l.Commitdate == Commitdate &&
                    l.Discount == Discount &&
                    l.Extendedprice == Extendedprice &&
                    l.Linenumber == Linenumber &&
                    l.Linestatus == Linestatus &&
                    l.Orderkey == Orderkey &&
                    l.Partkey == Partkey &&
                    l.Quantity == Quantity &&
                    l.Receiptdate == Receiptdate &&
                    l.Returnflag == Returnflag &&
                    l.Shipdate == Shipdate &&
                    l.Shipinstruct == Shipinstruct &&
                    l.Shipmode == Shipmode &&
                    l.Suppkey == Suppkey &&
                    l.Tax == Tax;
            }
            return false;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Orderkey, Partkey, Suppkey, Linenumber);
        }
    }

}
