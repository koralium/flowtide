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
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Sources.Generic;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.GenericDataTests
{
    internal class GenericDataTestStream : FlowtideTestStream
    {
        public GenericDataTestStream(string testName) : base(testName)
        {
        }

        protected override void AddReadResolvers(ReadWriteFactory factory)
        {
            factory.AddGenericDataSource("*", new TestDataSource());
        }
    }

    internal class TestDataSource : GenericDataSource<User>
    {
        public override TimeSpan? DeltaLoadInterval => TimeSpan.FromSeconds(1);

        public override async IAsyncEnumerable<FlowtideGenericObject<User>> DeltaLoadAsync(long lastWatermark)
        {
            yield return new FlowtideGenericObject<User>("2", new User { UserKey = 2, FirstName = "Test 2" }, 1, false);
            yield break;
        }

        public override async IAsyncEnumerable<FlowtideGenericObject<User>> FullLoadAsync()
        {
            yield return new FlowtideGenericObject<User>("1", new User { UserKey = 1, FirstName = "Test" }, 1, false);
        }
    }

    public class GenericReadOperatorTests
    {
        [Fact]
        public async Task TestGenericDataSource()
        {
            var stream = new GenericDataTestStream("TestGenericDataSource");
            stream.Generate();
            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    UserKey, 
                    FirstName 
                FROM users
            ");
            await stream.WaitForUpdate();
            var rows = stream.GetActualRowsAsVectors();
        }
    }
}
