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
using FlowtideDotNet.AcceptanceTests.Entities;
using static SqlParser.Ast.Statement;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    public class DatasetGenerator
    {
        private readonly MockDatabase mockDatabase;

        public List<User> Users { get; private set; }
        public List<Order> Orders { get; private set; }
        public List<Company> Companies { get; private set;}
        public List<Project> Projects { get; private set; }
        public List<ProjectMember> ProjectMembers { get; private set; }

        public DatasetGenerator(MockDatabase mockDatabase)
        {
            this.mockDatabase = mockDatabase;
            Users = new List<User>();
            Orders = new List<Order>();
            Companies = new List<Company>();
            Projects = new List<Project>();
            ProjectMembers = new List<ProjectMember>();

            Randomizer.Seed = new Random(8675309);
            mockDatabase.GetOrCreateTable<Company>("companies");
            mockDatabase.GetOrCreateTable<User>("users");
            mockDatabase.GetOrCreateTable<Order>("orders");
            mockDatabase.GetOrCreateTable<Project>("projects");
            mockDatabase.GetOrCreateTable<ProjectMember>("projectmembers");
        }

        public void Generate(int count = 1000, int seed = 8675309)
        {
            Randomizer.Seed = new Random(seed);
            GenerateCompanies(10);
            GenerateUsers(count);
            GenerateOrders(count);
            GenerateProjects(count);
            GenerateProjectMembers(count);
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

        public void AddOrUpdateOrder(Order order)
        {
            var index = Orders.FindIndex(x => x.OrderKey == order.OrderKey);

            if (index >= 0)
            {
                Orders[index] = order;
            }
            else
            {
                Orders.Add(order);
            }
            var mockTable = mockDatabase.GetOrCreateTable<Order>("orders");
            mockTable.AddOrUpdate(new List<Order>() { order });
        }

        public void AddOrUpdateCompany(Company company)
        {
            var index = Companies.FindIndex(x => x.CompanyId == company.CompanyId);

            if (index >= 0)
            {
                Companies[index] = company;
            }
            else
            {
                Companies.Add(company);
            }
            var mockTable = mockDatabase.GetOrCreateTable<Company>("companies");
            mockTable.AddOrUpdate(new List<Company>() { company });
        }

        public void DeleteUser(User user)
        {
            var index = Users.FindIndex(x => x.UserKey == user.UserKey);

            Users.RemoveAt(index);

            var mockTable = mockDatabase.GetOrCreateTable<User>("users");
            mockTable.Delete(new List<User>() { user });
        }

        public void DeleteOrder(Order order)
        {
            var index = Orders.FindIndex(x => x.OrderKey == order.OrderKey);

            Orders.RemoveAt(index);

            var mockTable = mockDatabase.GetOrCreateTable<Order>("orders");
            mockTable.Delete(new List<Order>() { order });
        }

        public void GenerateUsers(int count)
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
                .RuleFor(x => x.TrimmableNullableString, (f, u) => u.NullableString != null ? $" {u.NullableString} " : null)
                .RuleFor(x => x.DoubleValue, (f, u) => f.Random.Double(0, 1000))
                .RuleFor(x => x.Active, (f, u) => f.Random.Bool())
                .RuleFor(x => x.BirthDate, (f, u) => f.Date.BetweenDateOnly(DateOnly.Parse("1980-01-01"), DateOnly.Parse("2000-01-01")).ToDateTime(new TimeOnly(0)));


            var newUsers = testUsers.Generate(count);
            Users.AddRange(newUsers);

            var mockTable = mockDatabase.GetOrCreateTable<User>("users");
            mockTable.AddOrUpdate(newUsers);
        }

        public void GenerateOrders(int count)
        {
            var testOrders = new Faker<Order>()
                .RuleFor(x => x.OrderKey, (f, u) => f.UniqueIndex)
                .RuleFor(x => x.UserKey, (f, u) => f.PickRandom(Users).UserKey)
                .RuleFor(x => x.Orderdate, (f, u) => f.Date.Between(DateTime.Parse("1980-01-01"), DateTime.Parse("2000-01-01")))
                .RuleFor(x => x.GuidVal, (f, u) => Guid.NewGuid())
                .RuleFor(x => x.Money, (f, u) => f.Finance.Amount());

            var newOrders = testOrders.Generate(count);
            Orders.AddRange(newOrders);

            var mockTable = mockDatabase.GetOrCreateTable<Order>("orders");
            mockTable.AddOrUpdate(newOrders);
        }

        public void GenerateCompanies(int count)
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

        public void GenerateProjects(int count)
        {
            var testProjects = new Faker<Project>()
                .RuleFor(x => x.ProjectKey, (f, u) => f.UniqueIndex)
                .RuleFor(x => x.CompanyId, (f, u) => Companies[f.Random.Int(0, Companies.Count - 2)].CompanyId)
                .RuleFor(x => x.ProjectNumber, (f, u) => f.Random.AlphaNumeric(9))
                .RuleFor(x => x.Name, (f, u) => f.Commerce.ProductName());

            var newProjects = testProjects.Generate(count);
            Projects.AddRange(newProjects);

            var mockTable = mockDatabase.GetOrCreateTable<Project>("projects");
            mockTable.AddOrUpdate(newProjects);
        }

        public void GenerateProjectMembers(int count)
        {
            var testProjectMembers = new Faker<ProjectMember>()
                .StrictMode(false)
                .Rules((f, u) =>
                {
                    u.ProjectMemberKey = f.UniqueIndex;
                    var project = f.PickRandom(Projects);
                    u.CompanyId = project.CompanyId;
                    u.ProjectNumber = project.ProjectNumber;
                    var user = f.PickRandom(Users);
                    u.UserKey = user.UserKey;
                });

            var newProjectMembers = testProjectMembers.Generate(count);
            ProjectMembers.AddRange(newProjectMembers);
            var mockTable = mockDatabase.GetOrCreateTable<ProjectMember>("projectmembers");
            mockTable.AddOrUpdate(newProjectMembers);
        }
    }
}
