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

using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    public class DeleteStreamTests : FlowtideAcceptanceBase
    {
        public DeleteStreamTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper, true)
        {
        }

        [Fact]
        public async Task DeleteStreamAfterCheckpoint()
        {
            GenerateData();

            await StartStream("INSERT INTO output SELECT userkey, firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey, x.FirstName }));

            await DeleteStream();

            CancellationTokenSource tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            
            while (!tokenSource.IsCancellationRequested)
            {
                if (State == Base.Engine.Internal.StateMachine.StreamStateValue.Deleted)
                {
                    break;
                }
                if (State != Base.Engine.Internal.StateMachine.StreamStateValue.Deleting)
                {
                    throw new InvalidOperationException("Not in the deleting state");
                }
                
                await Task.Delay(100);
            }
        }
    }
}
