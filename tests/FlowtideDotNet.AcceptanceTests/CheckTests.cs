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

using FlowtideDotNet.Base.Engine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    public class CheckTests : FlowtideAcceptanceBase
    {
        public CheckTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        private class CheckFailureListener : ICheckFailureListener
        {
            public List<string> Errors { get; }

            public CheckFailureListener()
            {
                Errors = new List<string>();
            }

            public void OnCheckFailure(ref readonly CheckFailureNotification notification)
            {
                Errors.Add(Encoding.UTF8.GetString(notification.Utf8Message));
            }
        }

        [Fact]
        public async Task CheckValue()
        {
            GenerateData();

            var listener = new CheckFailureListener();
            await StartStream(@"
               INSERT INTO output 
                SELECT CHECK_VALUE(UserKey, UserKey < 900, concat('Userkey: ', UserKey, ' is too large')) 
                FROM users", failureListener: listener);

            await WaitForUpdate();

            var expected = Users.Where(x => x.UserKey >= 900)
                .Select(x => $"Userkey: {x.UserKey} is too large");

            Assert.Equal(expected, listener.Errors);

            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckTrue()
        {
            GenerateData();

            var listener = new CheckFailureListener();
            await StartStream(@"
               INSERT INTO output 
                SELECT 
                  UserKey
                FROM users
                WHERE CHECK_TRUE(userkey < 900, concat('Userkey: ', UserKey, ' is too large'))
            ", failureListener: listener);

            await WaitForUpdate();

            var expected = Users.Where(x => x.UserKey >= 900)
                .Select(x => $"Userkey: {x.UserKey} is too large");

            Assert.Equal(expected, listener.Errors);

            AssertCurrentDataEqual(Users.Where(x => x.UserKey < 900).Select(x => new { x.UserKey }));
        }
    }
}
