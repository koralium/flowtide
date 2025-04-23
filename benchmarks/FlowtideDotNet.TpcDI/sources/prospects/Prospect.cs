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

using CsvHelper.Configuration.Attributes;

namespace FlowtideDotNet.TpcDI.sources.prospects
{
    public class Prospect : IEquatable<Prospect>
    {
        [Index(0)]
        public string? AgencyID { get; set; }

        [Index(1)]
        public string? LastName { get; set; }

        [Index(2)]
        public string? FirstName { get; set; }

        [Index(3)]
        public string? MiddleInitial { get; set; }

        [Index(4)]
        public string? Gender { get; set; }

        [Index(5)]
        public string? AddressLine1 { get; set; }

        [Index(6)]
        public string? AddressLine2 { get; set; }

        [Index(7)]
        public string? PostalCode { get; set; }

        [Index(8)]
        public string? City { get; set; }

        [Index(9)]
        public string? State { get; set; }

        [Index(10)]
        public string? Country { get; set; }

        [Index(11)]
        public string? Phone { get; set; }

        [Index(12)]
        public decimal? Income { get; set; }

        [Index(13)]
        public int? NumberCars { get; set; }

        [Index(14)]
        public int? NumberChildren { get; set; }

        [Index(15)]
        public string? MaritalStatus { get; set; }

        [Index(16)]
        public int? Age { get; set; }

        [Index(17)]
        public int? CreditRating { get; set; }

        [Index(18)]
        public string? OwnOrRentFlag { get; set; }

        [Index(19)]
        public string? Employer { get; set; }

        [Index(20)]
        public int? NumberCreditCards { get; set; }

        [Index(21)]
        public decimal? NetWorth { get; set; }

        [Ignore]
        public DateTimeOffset RecordDate { get; set; }

        [Ignore]
        public DateTimeOffset UpdateDate { get; set; }

        [Ignore]
        public int BatchID { get; set; }

        public bool Equals(Prospect? other)
        {
            if (other == null) return false;

            return AgencyID == other.AgencyID &&
                LastName == other.LastName &&
                FirstName == other.FirstName &&
                MiddleInitial == other.MiddleInitial &&
                Gender == other.Gender &&
                AddressLine1 == other.AddressLine1 &&
                AddressLine2 == other.AddressLine2 &&
                PostalCode == other.PostalCode &&
                City == other.City &&
                State == other.State &&
                Country == other.Country &&
                Phone == other.Phone &&
                Income == other.Income &&
                NumberCars == other.NumberCars &&
                NumberChildren == other.NumberChildren &&
                MaritalStatus == other.MaritalStatus &&
                Age == other.Age &&
                CreditRating == other.CreditRating &&
                OwnOrRentFlag == other.OwnOrRentFlag &&
                Employer == other.Employer &&
                NumberCreditCards == other.NumberCreditCards &&
                NetWorth == other.NetWorth;
        }
    }
}
