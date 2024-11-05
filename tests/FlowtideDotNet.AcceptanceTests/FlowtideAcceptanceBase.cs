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

using FlexBuffers;
using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Substrait.Sql;
using System.Diagnostics;
using System.Reflection;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    public class FlowtideAcceptanceBase : IAsyncLifetime
    {
        private readonly FlowtideTestStream flowtideTestStream;

        public IReadOnlyList<User> Users => flowtideTestStream.Users;
        public IReadOnlyList<Order> Orders => flowtideTestStream.Orders;
        public IReadOnlyList<Company> Companies => flowtideTestStream.Companies;
        public IReadOnlyList<Project> Projects => flowtideTestStream.Projects;
        public IReadOnlyList<ProjectMember> ProjectMembers => flowtideTestStream.ProjectMembers;
        public IFunctionsRegister FunctionsRegister => flowtideTestStream.FunctionsRegister;
        public ISqlFunctionRegister SqlFunctionRegister => flowtideTestStream.SqlFunctionRegister;

        protected Task StartStream(
            string sql, 
            int parallelism = 1, 
            StateSerializeOptions? stateSerializeOptions = default, 
            int pageSize = 1024,
            bool ignoreSameDataCheck = false) => flowtideTestStream.StartStream(sql, parallelism, stateSerializeOptions, default, pageSize, ignoreSameDataCheck);

        public List<FlxVector> GetActualRows() => flowtideTestStream.GetActualRowsAsVectors();

        protected void AssertCurrentDataEqual<T>(IEnumerable<T> data)
        {
            flowtideTestStream.AssertCurrentDataEqual(data);
        }

        protected void GenerateData(int count = 1000)
        {
            flowtideTestStream.Generate(count);
        }

        protected void GenerateUsers(int count = 1000)
        {
            flowtideTestStream.GenerateUsers(count);
        }

        protected void GenerateOrders(int count = 1000)
        {
            flowtideTestStream.GenerateOrders(count);
        }

        protected void GenerateCompanies(int count = 1)
        {
            flowtideTestStream.GenerateCompanies(count);
        }

        protected void GenerateProjects(int count = 1000)
        {
            flowtideTestStream.GenerateProjects(count);
        }

        protected void GenerateProjectMembers(int count = 1000)
        {
            flowtideTestStream.GenerateProjectMembers(count);
        }

        protected void AddOrUpdateCompany(Company company)
        {
            flowtideTestStream.AddOrUpdateCompany(company);
        }

        protected Task WaitForUpdate()
        {
            return flowtideTestStream.WaitForUpdate();
        }

        protected Task Crash()
        {
            return flowtideTestStream.Crash();
        }

        protected void EgressCrashOnCheckpoint(int times)
        {
            flowtideTestStream.EgressCrashOnCheckpoint(times);
        }

        public void AddOrUpdateUser(User user)
        {
            flowtideTestStream.AddOrUpdateUser(user);
        }

        public void DeleteUser(User user)
        {
            flowtideTestStream.DeleteUser(user);
        }

        public void DeleteOrder(Order order)
        {
            flowtideTestStream.DeleteOrder(order);
        }

        public FlowtideAcceptanceBase(ITestOutputHelper testOutputHelper)
        {
            var baseType = this.GetType();
            var testName = GetTestClassName(testOutputHelper);
            flowtideTestStream = new FlowtideTestStream($"{baseType.Name}/{testName}");
            //flowtideTestStream.CachePageCount = 10;
        }

        private static string GetTestClassName(ITestOutputHelper output)
        {
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            Debug.Assert(testMember != null, "testMember != null");
            var test = (ITest?)testMember.GetValue(output);
            Debug.Assert(test != null, "test != null");
            return test.TestCase.TestMethod.Method.Name;
        }

        public async Task DisposeAsync()
        {
            
            await flowtideTestStream.DisposeAsync();
        }

        public Task InitializeAsync()
        {
            return Task.CompletedTask;
        }
    }
}
