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

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    public class DatasetGenerator
    {
        private readonly MockDatabase mockDatabase;

        public List<User> Users { get; private set; }
        public List<Order> Orders { get; private set; }

        public DatasetGenerator(MockDatabase mockDatabase)
        {
            this.mockDatabase = mockDatabase;
        }

        public void Generate(int count = 1000, int seed = 8675309)
        {
            Randomizer.Seed = new Random(seed);
            GenerateUsers(count);
            GenerateOrders(count);
        }

        private void GenerateUsers(int count)
        {
            var testUsers = new Faker<User>()
                .RuleFor(x => x.UserKey, (f, u) => f.UniqueIndex)
                .RuleFor(x => x.Gender, (f, u) => f.PickRandom<Gender>())
                .RuleFor(x => x.FirstName, (f, u) => f.Name.FirstName((Bogus.DataSets.Name.Gender)u.Gender))
                .RuleFor(x => x.LastName, (f, u) => f.Name.LastName((Bogus.DataSets.Name.Gender)u.Gender));


            Users = testUsers.Generate(count);

            var mockTable = mockDatabase.GetOrCreateTable<User>("users");
            mockTable.AddOrUpdate(Users);
        }

        private void GenerateOrders(int count)
        {
            var testOrders = new Faker<Order>()
                .RuleFor(x => x.OrderKey, (f, u) => f.UniqueIndex)
                .RuleFor(x => x.UserKey, (f, u) => f.PickRandom(Users).UserKey)
                .RuleFor(x => x.Orderdate, (f, u) => f.Date.Between(DateTime.Parse("1980-01-01"), DateTime.Parse("2000-01-01")));

            Orders = testOrders.Generate(count);

            var mockTable = mockDatabase.GetOrCreateTable<Order>("orders");
            mockTable.AddOrUpdate(Orders);
        }

        private void ChangeTracker_StateChanged(object? sender, Microsoft.EntityFrameworkCore.ChangeTracking.EntityStateChangedEventArgs e)
        {
            throw new NotImplementedException();
        }
    }
}
