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
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Substrait.Sql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.AcceptanceTests
{
    public class FlowtideAcceptanceBase : IAsyncLifetime
    {
        private readonly FlowtideTestStream flowtideTestStream;

        public IReadOnlyList<User> Users => flowtideTestStream.Users;
        public IReadOnlyList<Order> Orders => flowtideTestStream.Orders;
        public IFunctionsRegister FunctionsRegister => flowtideTestStream.FunctionsRegister;
        public ISqlFunctionRegister SqlFunctionRegister => flowtideTestStream.SqlFunctionRegister;

        protected Task StartStream(string sql) => flowtideTestStream.StartStream(sql);

        protected void AssertCurrentDataEqual<T>(IEnumerable<T> data)
        {
            flowtideTestStream.AssertCurrentDataEqual(data);
        }

        protected void GenerateData(int count = 1000)
        {
            flowtideTestStream.Generate(count);
        }

        protected Task WaitForUpdate()
        {
            return flowtideTestStream.WaitForUpdate();
        }

        public FlowtideAcceptanceBase()
        {
            flowtideTestStream = new FlowtideTestStream();
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
