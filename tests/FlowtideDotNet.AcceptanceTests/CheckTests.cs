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
                Errors.Add(notification.Message);
            }
        }

        private class CheckFailureReplaceListener : ICheckFailureListener
        {
            public List<string> Errors { get; }

            public CheckFailureReplaceListener()
            {
                Errors = new List<string>();
            }

            public void OnCheckFailure(ref readonly CheckFailureNotification notification)
            {
                var msg = notification.Message;
                for (int i = 0; i < notification.Tags.Length; i++)
                {
                    msg = msg.Replace($"{{{notification.Tags[i].Key}}}", notification.Tags[i].Value?.ToString() ?? "null", StringComparison.OrdinalIgnoreCase);
                }
                Errors.Add(msg);
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

        [Fact]
        public async Task CheckValueWithTags()
        {
            GenerateData();

            var listener = new CheckFailureReplaceListener();
            await StartStream(@"
               INSERT INTO output 
                SELECT CHECK_VALUE(UserKey, UserKey < 900, 'Userkey: {userkey} is too large', userkey => UserKey) 
                FROM users", failureListener: listener);

            await WaitForUpdate();

            var expected = Users.Where(x => x.UserKey >= 900)
                .Select(x => $"Userkey: {x.UserKey} is too large");

            Assert.Equal(expected, listener.Errors);

            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckValueWithTagsNullValue()
        {
            GenerateData();

            var listener = new CheckFailureReplaceListener();
            await StartStream(@"
               INSERT INTO output 
                SELECT CHECK_VALUE(UserKey, UserKey < 900, 'Userkey: {userkey} is too large', userkey => null) 
                FROM users", failureListener: listener);

            await WaitForUpdate();

            var expected = Users.Where(x => x.UserKey >= 900)
                .Select(x => $"Userkey: null is too large");

            Assert.Equal(expected, listener.Errors);

            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckValueWithTagsNonNamed()
        {
            GenerateData();

            var listener = new CheckFailureReplaceListener();
            await StartStream(@"
               INSERT INTO output 
                SELECT CHECK_VALUE(UserKey, UserKey < 900, 'Userkey: {userkey} is too large', userkey) 
                FROM users", failureListener: listener);

            await WaitForUpdate();

            var expected = Users.Where(x => x.UserKey >= 900)
                .Select(x => $"Userkey: {x.UserKey} is too large");

            Assert.Equal(expected, listener.Errors);

            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckValueWithWindowFunction()
        {
            GenerateData();

            var listener = new CheckFailureReplaceListener();
            await StartStream(@"
               INSERT INTO output 
                SELECT CHECK_VALUE(UserKey, ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey) = 1, 'Duplicate user: {userkey} found for company {companyId}', userkey, companyId) 
                FROM users", failureListener: listener);

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(x =>
                {
                    bool first = true;
                    List<string> output = new List<string>();

                    foreach (var row in x)
                    {
                        if (first)
                        {
                            first = false;
                            continue;
                        }
                        output.Add($"Duplicate user: {row.UserKey} found for company {row.CompanyId ?? "null"}");
                    }
                    return output;
                }).ToList();

            Assert.Equal(expected.OrderBy(x => x).ToList(), listener.Errors.OrderBy(x => x).ToList());

            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckTrueWithTags()
        {
            GenerateData();

            var listener = new CheckFailureReplaceListener();
            await StartStream(@"
               INSERT INTO output 
                SELECT 
                  UserKey
                FROM users
                WHERE CHECK_TRUE(userkey < 900, 'Userkey: {userkey} is too large', userkey => UserKey)
            ", failureListener: listener);

            await WaitForUpdate();

            var expected = Users.Where(x => x.UserKey >= 900)
                .Select(x => $"Userkey: {x.UserKey} is too large");

            Assert.Equal(expected, listener.Errors);

            AssertCurrentDataEqual(Users.Where(x => x.UserKey < 900).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckTrueWithTagsNoNamed()
        {
            GenerateData();

            var listener = new CheckFailureReplaceListener();
            await StartStream(@"
               INSERT INTO output 
                SELECT 
                  UserKey
                FROM users
                WHERE CHECK_TRUE(userkey < 900, 'Userkey: {userkey} is too large', UserKey)
            ", failureListener: listener);

            await WaitForUpdate();

            var expected = Users.Where(x => x.UserKey >= 900)
                .Select(x => $"Userkey: {x.UserKey} is too large");

            Assert.Equal(expected, listener.Errors);

            AssertCurrentDataEqual(Users.Where(x => x.UserKey < 900).Select(x => new { x.UserKey }));
        }
    }
}
