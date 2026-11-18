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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace FlowtideDotNet.AcceptanceTests.MultiJoin
{
    // The row budget is one for the whole process, nothing else may run while a test here changes it
    [CollectionDefinition("Multi join row budget", DisableParallelization = true)]
    public class MultiJoinRowBudgetCollection
    {
    }

    [Collection("Multi join row budget")]
    public class MultiJoinDifferentialTests
    {
        private readonly ITestOutputHelper _output;

        public MultiJoinDifferentialTests(ITestOutputHelper output)
        {
            _output = output;
        }

        public enum Arrival
        {
            KeyOrdered,
            Reverse,
            Shuffled
        }

        private static readonly Dictionary<string, string> Templates = new Dictionary<string, string>()
        {
            ["chain3"] = @"
                INSERT INTO output
                SELECT o.orderkey, u.firstName, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                INNER JOIN companies c ON u.companyId = c.companyId",
            ["star4"] = @"
                INSERT INTO output
                SELECT pm.projectMemberKey, u.firstName, p.name, c.name
                FROM projectmembers pm
                INNER JOIN users u ON pm.userkey = u.userkey
                INNER JOIN projects p ON pm.projectNumber = p.projectNumber AND pm.companyId = p.companyId
                INNER JOIN companies c ON pm.companyId = c.companyId",
            ["mixed5"] = @"
                INSERT INTO output
                SELECT pm.projectMemberKey, u.firstName, c.name, p.name, o.orderkey
                FROM projectmembers pm
                INNER JOIN users u ON pm.userkey = u.userkey
                INNER JOIN companies c ON u.companyId = c.companyId
                INNER JOIN projects p ON pm.projectNumber = p.projectNumber AND pm.companyId = p.companyId
                INNER JOIN orders o ON o.userkey = u.userkey",
            // The second join has its key on the padded side of the first
            ["leftChain3"] = @"
                INSERT INTO output
                SELECT o.orderkey, u.firstName, c.name
                FROM orders o
                LEFT JOIN users u ON o.userkey = u.userkey
                LEFT JOIN companies c ON u.companyId = c.companyId",
            ["leftStar4"] = @"
                INSERT INTO output
                SELECT pm.projectMemberKey, u.firstName, p.name, c.name
                FROM projectmembers pm
                LEFT JOIN users u ON pm.userkey = u.userkey
                LEFT JOIN projects p ON pm.projectNumber = p.projectNumber AND pm.companyId = p.companyId
                LEFT JOIN companies c ON pm.companyId = c.companyId",
            // An inner join above a left join, keyed on the preserved side.
            ["leftThenInner3"] = @"
                INSERT INTO output
                SELECT pm.projectMemberKey, u.firstName, c.name
                FROM projectmembers pm
                LEFT JOIN users u ON pm.userkey = u.userkey
                INNER JOIN companies c ON pm.companyId = c.companyId",
            // An inner join keyed on the padded side of the left join below it
            ["leftThenInnerOnPadded3"] = @"
                INSERT INTO output
                SELECT o.orderkey, u.firstName, c.name
                FROM orders o
                LEFT JOIN users u ON o.userkey = u.userkey
                INNER JOIN companies c ON u.companyId = c.companyId",
            // The same above a full outer join, where both sides are padded
            ["outerThenInner3"] = @"
                INSERT INTO output
                SELECT o.orderkey, u.firstName, c.name
                FROM orders o
                FULL OUTER JOIN users u ON o.userkey = u.userkey
                INNER JOIN companies c ON u.companyId = c.companyId",
            // With three inputs per multi join the left join is over the cap and keeps its filter
            ["leftThenInnerOnPadded4"] = @"
                INSERT INTO output
                SELECT o.orderkey, u.firstName, c.name, pm.projectMemberKey
                FROM orders o
                LEFT JOIN users u ON o.userkey = u.userkey
                INNER JOIN companies c ON u.companyId = c.companyId
                INNER JOIN projectmembers pm ON pm.userkey = o.userkey",
            // A right join keyed on the padded side of the left join below it
            ["leftThenRightOnPadded3"] = @"
                INSERT INTO output
                SELECT o.orderkey, u.firstName, c.name
                FROM orders o
                LEFT JOIN users u ON o.userkey = u.userkey
                RIGHT JOIN companies c ON u.companyId = c.companyId",
            ["leftMixed5"] = @"
                INSERT INTO output
                SELECT pm.projectMemberKey, u.firstName, c.name, p.name, o.orderkey
                FROM projectmembers pm
                INNER JOIN users u ON pm.userkey = u.userkey
                LEFT JOIN companies c ON u.companyId = c.companyId
                LEFT JOIN projects p ON pm.projectNumber = p.projectNumber AND pm.companyId = p.companyId
                LEFT JOIN orders o ON o.userkey = u.userkey",
            ["rightChain3"] = @"
                INSERT INTO output
                SELECT o.orderkey, u.firstName, c.name
                FROM orders o
                RIGHT JOIN users u ON o.userkey = u.userkey
                RIGHT JOIN companies c ON u.companyId = c.companyId",
            ["outerChain3"] = @"
                INSERT INTO output
                SELECT o.orderkey, u.firstName, c.name
                FROM orders o
                FULL OUTER JOIN users u ON o.userkey = u.userkey
                FULL OUTER JOIN companies c ON u.companyId = c.companyId",
            // A full outer join below an inner join and a left join
            ["outerMixed4"] = @"
                INSERT INTO output
                SELECT pm.projectMemberKey, u.firstName, c.name, o.orderkey
                FROM projectmembers pm
                FULL OUTER JOIN users u ON pm.userkey = u.userkey
                INNER JOIN companies c ON pm.companyId = c.companyId
                LEFT JOIN orders o ON o.userkey = pm.userkey",
            ["exists3"] = @"
                INSERT INTO output
                SELECT o.orderkey, u.firstName
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                WHERE EXISTS (SELECT 1 FROM projectmembers pm WHERE pm.userkey = u.userkey)",
            ["notExistsLeft3"] = @"
                INSERT INTO output
                SELECT o.orderkey, u.firstName
                FROM orders o
                LEFT JOIN users u ON o.userkey = u.userkey
                WHERE NOT EXISTS (SELECT 1 FROM projectmembers pm WHERE pm.userkey = o.userkey)",
            // Conditions beside the keys, checked after the lookup
            ["checkChain3"] = @"
                INSERT INTO output
                SELECT o.orderkey, u.firstName, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey AND o.orderkey <> u.visits
                INNER JOIN companies c ON u.companyId = c.companyId AND c.name <> u.firstName",
            // The condition of the inner join reads the padded side of the left join below it
            ["checkOverLeft3"] = @"
                INSERT INTO output
                SELECT pm.projectMemberKey, u.firstName, c.name
                FROM projectmembers pm
                LEFT JOIN users u ON pm.userkey = u.userkey
                INNER JOIN companies c ON pm.companyId = c.companyId AND u.firstName <> c.name",
            // Orders failing the condition on them are padded whatever users holds
            ["leftOnly3"] = @"
                INSERT INTO output
                SELECT o.orderkey, u.firstName, c.name
                FROM orders o
                LEFT JOIN users u ON o.userkey = u.userkey AND o.orderkey > 60
                LEFT JOIN companies c ON u.companyId = c.companyId",
            // A condition on the kept side of a right join
            ["rightOnly3"] = @"
                INSERT INTO output
                SELECT o.orderkey, u.firstName, c.lastName
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                RIGHT JOIN users c ON u.managerKey = c.userkey AND c.visits > 3",
            // A condition on one input of the joined side of a left join
            ["leftFilteredSide3"] = @"
                INSERT INTO output
                SELECT c.name, u.firstName, o.orderkey
                FROM companies c
                LEFT JOIN users u ON c.companyId = u.companyId AND u.visits > 3
                LEFT JOIN orders o ON u.userkey = o.userkey",
        };

        // More seeds for a qualification run: FLOWTIDE_MULTIJOIN_SEEDS=200
        private static int SeedCount => int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_SEEDS"), out var seeds) ? seeds : 2;

        public static IEnumerable<object[]> Cases()
        {
            foreach (var template in Templates.Keys)
            {
                foreach (var arrival in Enum.GetValues<Arrival>())
                {
                    for (int seed = 1; seed <= SeedCount; seed++)
                    {
                        yield return new object[] { template, arrival, 5, MultiJoinOperator.DefaultRowBudget, seed };
                    }
                }
                // Several multi joins in a row, and every slice boundary that can exist
                yield return new object[] { template, Arrival.Shuffled, 3, MultiJoinOperator.DefaultRowBudget, 1 };
                yield return new object[] { template, Arrival.Shuffled, 5, 1, 1 };
            }
        }

        private static List<T> InArrivalOrder<T>(IReadOnlyList<T> items, Arrival arrival, Random random)
        {
            var list = items.ToList();
            if (arrival == Arrival.Reverse)
            {
                list.Reverse();
            }
            else if (arrival == Arrival.Shuffled)
            {
                for (int i = list.Count - 1; i > 0; i--)
                {
                    int j = random.Next(i + 1);
                    (list[i], list[j]) = (list[j], list[i]);
                }
            }
            return list;
        }

        private static async Task WaitForEqualResults(FlowtideTestStream chain, FlowtideTestStream multi, string step)
        {
            // Compared once both sinks have published rows that include every change made to the tables
            var watch = System.Diagnostics.Stopwatch.StartNew();
            var sinceAsked = System.Diagnostics.Stopwatch.StartNew();
            while (!chain.PublishedDataIsCurrent() || !multi.PublishedDataIsCurrent())
            {
                Assert.True(watch.Elapsed < TimeSpan.FromSeconds(60), $"The changes of {step} did not reach both sinks. Chain: {chain.DescribePublishedOffsets()}. Multi join: {multi.DescribePublishedOffsets()}");
                await chain.SchedulerTick();
                await multi.SchedulerTick();
                await Task.Delay(10);
                if (sinceAsked.Elapsed > TimeSpan.FromMilliseconds(500))
                {
                    // A restarted source says where it stands only when it has changes, here every source is asked to
                    sinceAsked.Restart();
                    await chain.Trigger("send_empty_batch");
                    await multi.Trigger("send_empty_batch");
                }
            }
            try
            {
                EventBatchAssertion.Equal(chain.GetActualRowsAsVectors(), multi.GetActualRowsAsVectors());
            }
            catch (XunitException e)
            {
                Assert.Fail($"The multi join result differs from the join chain after {step}: {e.Message}");
            }
        }

        [Theory]
        [MemberData(nameof(Cases))]
        public async Task SameResultAsTheJoinChain(string template, Arrival arrival, int maxInputs, int rowBudget, int seed)
        {
            var random = new Random(seed * 7919 + template.Length);
            string name = $"MultiJoinDifferential/{template}_{arrival}_{maxInputs}_{rowBudget}_{seed}";

            // Only generates the rows, both streams get the same ones in the same order
            var source = new FlowtideTestStream(name + "/source");
            source.Generate(150);
            source.GenerateCompanies(4);
            source.GenerateProjects(30);
            source.GenerateProjectMembers(150);

            int budgetBefore = MultiJoinOperator.RowBudget;
            MultiJoinOperator.RowBudget = rowBudget;
            int minRowsBefore = MultiJoinOperator.ParallelInsertMinRows;
            // Half the seeds store every batch beside its join, also through the crash and the recovery
            MultiJoinOperator.ParallelInsertMinRows = seed % 2 == 0 ? 1 : minRowsBefore;
            // The other seeds look every later join up beside the one before it, as when a lookup waits for a page
            bool lookupsBefore = MultiJoinOperator.ParallelLookupsAlways;
            MultiJoinOperator.ParallelLookupsAlways = seed % 2 == 1;
            await using var chain = new FlowtideTestStream(name + "/chain") { MultiJoinMaxInputs = 0 };
            await using var multi = new FlowtideTestStream(name + "/multi") { MultiJoinMaxInputs = maxInputs };
            try
            {
                var streams = new[] { chain, multi };
                var users = InArrivalOrder(source.Users, arrival, random);
                var orders = InArrivalOrder(source.Orders, arrival, random);
                var companies = InArrivalOrder(source.Companies, arrival, random);
                var projects = InArrivalOrder(source.Projects, arrival, random);
                var members = InArrivalOrder(source.ProjectMembers, arrival, random);
                foreach (var stream in streams)
                {
                    foreach (var company in companies) stream.AddOrUpdateCompany(company);
                    foreach (var user in users) stream.AddUser(user);
                    foreach (var order in orders) stream.AddOrder(order);
                    stream.AddProjects(projects.ToArray());
                    stream.AddProjectMembers(members.ToArray());
                }

                await Task.WhenAll(
                    chain.StartStream(Templates[template], ignoreSameDataCheck: true),
                    multi.StartStream(Templates[template], ignoreSameDataCheck: true));
                Assert.True(multi.MultiJoinGroupsMarked > 0, "The query did not become a multi join");
                Assert.Equal(0, chain.MultiJoinGroupsMarked);
                await WaitForEqualResults(chain, multi, "the initial load");

                var removedUsers = new List<User>();
                int knownOrders = source.Orders.Count;
                for (int round = 1; round <= 6; round++)
                {
                    var steps = new List<string>();
                    int changes = random.Next(3, 7);
                    for (int i = 0; i < changes; i++)
                    {
                        // The first change of a round always happens, so every round ends in a checkpoint
                        switch (i == 0 ? 0 : random.Next(9))
                        {
                            case 0:
                                {
                                    var user = users[random.Next(users.Count)];
                                    user.FirstName = "name" + random.Next(1000);
                                    foreach (var stream in streams) stream.AddOrUpdateUser(user);
                                    steps.Add("user payload");
                                    break;
                                }
                            case 1:
                                {
                                    // A key change on an input that is looked up by two keys
                                    var user = users[random.Next(users.Count)];
                                    user.CompanyId = companies[random.Next(companies.Count)].CompanyId;
                                    foreach (var stream in streams) stream.AddOrUpdateUser(user);
                                    steps.Add("user company");
                                    break;
                                }
                            case 2:
                                if (users.Count > 20)
                                {
                                    var user = users[random.Next(users.Count)];
                                    users.Remove(user);
                                    removedUsers.Add(user);
                                    foreach (var stream in streams) stream.DeleteUser(user);
                                    steps.Add("user delete");
                                }
                                break;
                            case 3:
                                if (removedUsers.Count > 0)
                                {
                                    var user = removedUsers[random.Next(removedUsers.Count)];
                                    removedUsers.Remove(user);
                                    users.Add(user);
                                    foreach (var stream in streams) stream.AddUser(user);
                                    steps.Add("user back");
                                }
                                break;
                            case 4:
                                if (orders.Count > 20)
                                {
                                    var order = orders[random.Next(orders.Count)];
                                    orders.Remove(order);
                                    foreach (var stream in streams) stream.DeleteOrder(order);
                                    steps.Add("order delete");
                                }
                                break;
                            case 5:
                                {
                                    source.GenerateOrders(random.Next(1, 40));
                                    for (int o = knownOrders; o < source.Orders.Count; o++)
                                    {
                                        orders.Add(source.Orders[o]);
                                        foreach (var stream in streams) stream.AddOrder(source.Orders[o]);
                                    }
                                    knownOrders = source.Orders.Count;
                                    steps.Add("orders added");
                                    break;
                                }
                            case 6:
                                {
                                    var company = companies[random.Next(companies.Count)];
                                    company.Name = "company" + random.Next(1000);
                                    foreach (var stream in streams) stream.AddOrUpdateCompany(company);
                                    steps.Add("company payload");
                                    break;
                                }
                            case 7:
                                {
                                    var project = projects[random.Next(projects.Count)];
                                    project.Name = "project" + random.Next(1000);
                                    foreach (var stream in streams) stream.AddOrUpdateProject(project);
                                    steps.Add("project payload");
                                    break;
                                }
                            case 8:
                                {
                                    var member = members[random.Next(members.Count)];
                                    member.UserKey = source.Users[random.Next(source.Users.Count)].UserKey;
                                    foreach (var stream in streams) stream.AddOrUpdateProjectMember(member);
                                    steps.Add("member user");
                                    break;
                                }
                        }
                    }
                    if (round == 3)
                    {
                        // The changes of this round are still on their way when both streams go down
                        await Task.WhenAll(chain.Crash(), multi.Crash());
                        steps.Add("crash");
                    }
                    string description = $"round {round} ({string.Join(", ", steps)})";
                    _output.WriteLine(description);
                    await WaitForEqualResults(chain, multi, description);
                }
                Assert.True(chain.GetActualRowsAsVectors().Count > 0, "The query gave no rows, the comparison says nothing");
            }
            finally
            {
                MultiJoinOperator.RowBudget = budgetBefore;
                MultiJoinOperator.ParallelInsertMinRows = minRowsBefore;
                MultiJoinOperator.ParallelLookupsAlways = lookupsBefore;
            }
        }
    }
}
