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

using AspireSamples.Entities;
using Bogus;

namespace AspireSamples.DataMigration
{
    internal class DataGenerator
    {
        public List<User> Users { get; private set; }
        public List<Order> Orders { get; private set; }

        public DataGenerator()
        {
            Users = new List<User>();
            Orders = new List<Order>();

            Randomizer.Seed = new Random(8675309);
        }

        public List<User> GenerateUsers(int count)
        {
            var testUsers = new Faker<User>()
                .RuleFor(x => x.UserKey, (f, u) => f.UniqueIndex + 1)
                .RuleFor(x => x.FirstName, (f, u) => f.Name.FirstName())
                .RuleFor(x => x.LastName, (f, u) => f.Name.LastName());

            var newUsers = testUsers.Generate(count);

            Users.AddRange(newUsers);

            return newUsers;
        }

        public List<Order> GenerateOrders(int count)
        {
            var testOrders = new Faker<Order>()
                .RuleFor(x => x.OrderKey, (f, o) => f.UniqueIndex + 1)
                .RuleFor(x => x.UserKey, (f, o) => f.PickRandom(Users).UserKey)
                .RuleFor(x => x.Orderdate, (f, o) => f.Date.Past());

            var newOrders = testOrders.Generate(count);

            Orders.AddRange(newOrders);

            return newOrders;
        }
    }
}
