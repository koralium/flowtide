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

namespace FlowtideDotNet.AcceptanceTests.Entities
{
    public class User
    {
        [Key]
        public int UserKey { get; set; }

        public Gender Gender { get; set; }

        public string? FirstName { get; set; }

        public string? LastName { get; set; }

        public string? NullableString { get; set; }

        public string? CompanyId { get; set; }

        public int? Visits { get; set; }

        public int? ManagerKey { get; set; }

        public string? TrimmableNullableString { get; set; }

        public double DoubleValue { get; set; }

        public bool Active { get; set; }

        public DateTime? BirthDate { get; set; }
    }
}
