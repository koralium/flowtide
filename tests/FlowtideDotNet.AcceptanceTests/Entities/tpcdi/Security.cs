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

using System.ComponentModel.DataAnnotations;

namespace FlowtideDotNet.AcceptanceTests.Entities.tpcdi
{
    public class Security
    {
        [Key]
        public string? Key { get; set; }

        public DateTime PostingDate { get; set; }

        public string? Symbol { get; set; }

        public string? IssueType { get; set; }

        public string? Status { get; set; }

        public string? Name { get; set; }

        public string? ExID { get; set; }

        public string? ShOut { get; set; }

        public string? FirstTradeDate { get; set; }

        public string? FirstTradeExchg { get; set; }

        public string? Dividend { get; set; }

        public string? CoNameOrCIK { get; set; }

        public int BatchID { get; set; }
    }
}
