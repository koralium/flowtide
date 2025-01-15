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

using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    public class StreamStateTests : FlowtideAcceptanceBase
    {
        public StreamStateTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task TestStopStream()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT * FROM users");

            await WaitForUpdate();

            await StopStream();

            Assert.Equal(StreamStateValue.NotStarted, State);
        }

        //[Fact]
        //public async Task TestStopStreamThenStart()
        //{
        //    GenerateData();
        //    await StartStream(@"
        //    INSERT INTO output
        //    SELECT userkey FROM users");

        //    await WaitForUpdate();

        //    await StopStream();

        //    Assert.Equal(StreamStateValue.NotStarted, State);

        //    GenerateData();

        //    await StartStream();

        //    await WaitForUpdate();

        //    AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));

        //    Assert.Equal(StreamStateValue.Running, State);
        //}
    }
}
