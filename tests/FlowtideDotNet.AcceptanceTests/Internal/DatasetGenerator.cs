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

using Bogus;
using FastMember;
using FlexBuffers;
using FlowtideDotNet.AcceptanceTests.Entities;
using Microsoft.EntityFrameworkCore;
using System.Data.Common;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    public class DatasetGenerator
    {
        private readonly MockDatabase mockDatabase;

        public List<User> Users { get; private set; }
        public List<Order> Orders { get; private set; }
        public List<Company> Companies { get; private set;}

        public DatasetGenerator(MockDatabase mockDatabase)
        {
            this.mockDatabase = mockDatabase;
            Users = new List<User>();
            Orders = new List<Order>();
            Companies = new List<Company>();
        }

        public void Generate(int count = 1000, int seed = 8675309)
        {
            Randomizer.Seed = new Random(seed);
            GenerateCompanies(1);
            GenerateUsers(count);
            GenerateOrders(count);
        }

        public void AddOrUpdateUser(User user)
        {
            var index = Users.FindIndex(x => x.UserKey == user.UserKey);

            if (index >= 0)
            {
                Users[index] = user;
            }
            else
            {
                Users.Add(user);
            }
            var mockTable = mockDatabase.GetOrCreateTable<User>("users");
            mockTable.AddOrUpdate(new List<User>() { user });
        }

        private void GenerateUsers(int count)
        {
            string?[] nullableStrings = new string?[] { null, "value" };

            List<int> availableManagers = new List<int>();

            var testUsers = new Faker<User>()
                .RuleFor(x => x.UserKey, (f, u) => f.UniqueIndex)
                .RuleFor(x => x.Gender, (f, u) => f.PickRandom<Gender>())
                .RuleFor(x => x.FirstName, (f, u) => f.Name.FirstName((Bogus.DataSets.Name.Gender)u.Gender))
                .RuleFor(x => x.LastName, (f, u) => f.Name.LastName((Bogus.DataSets.Name.Gender)u.Gender))
                .RuleFor(x => x.NullableString, (f, u) => f.PickRandom(nullableStrings))
                .RuleFor(x => x.CompanyId, (f, u) =>
                {
                    if (f.Random.Bool())
                    {
                        return f.PickRandom(Companies).CompanyId;
                    }
                    return null;
                })
                .RuleFor(x => x.Visits, (f, u) => f.Random.Number(1, 10).OrNull(f))
                .RuleFor(x => x.ManagerKey, (f, u) =>
                {
                    if (availableManagers.Count == 0)
                    {
                        availableManagers.Add(u.UserKey);
                        return default(int?);
                    }
                    else
                    {
                        var managerKey = f.PickRandom(availableManagers);
                        availableManagers.Add(u.UserKey);
                        return managerKey;
                    }
                })
                .RuleFor(x => x.TrimmableNullableString, (f, u) => u.NullableString != null ? $" {u.NullableString} " : null);


            var newUsers = testUsers.Generate(count);
            Users.AddRange(newUsers);

            var mockTable = mockDatabase.GetOrCreateTable<User>("users");
            mockTable.AddOrUpdate(newUsers);
        }

        private void GenerateOrders(int count)
        {
            var testOrders = new Faker<Order>()
                .RuleFor(x => x.OrderKey, (f, u) => f.UniqueIndex)
                .RuleFor(x => x.UserKey, (f, u) => f.PickRandom(Users).UserKey)
                .RuleFor(x => x.Orderdate, (f, u) => f.Date.Between(DateTime.Parse("1980-01-01"), DateTime.Parse("2000-01-01")))
                .RuleFor(x => x.GuidVal, (f, u) => Guid.NewGuid());

            var newOrders = testOrders.Generate(count);
            Orders.AddRange(newOrders);

            var mockTable = mockDatabase.GetOrCreateTable<Order>("orders");
            mockTable.AddOrUpdate(newOrders);
        }

        private void GenerateCompanies(int count)
        {
            var testCompanies = new Faker<Company>()
                .RuleFor(x => x.CompanyId, (f, u) => $"{f.Company.Random.AlphaNumeric(5)}-{f.Company.Random.AlphaNumeric(3)}")
                .RuleFor(x => x.Name, (f, u) => f.Company.CompanyName());

            var newCompanies = testCompanies.Generate(count);
            
            // Add a null company for unit testing, useful for Kleene logic testing
            newCompanies.Add(new Company()
            {
                CompanyId = null,
                Name = "Null company"
            });
            Companies.AddRange(newCompanies);
            var mockTable = mockDatabase.GetOrCreateTable<Company>("companies");
            mockTable.AddOrUpdate(newCompanies);
        }
    }
}
