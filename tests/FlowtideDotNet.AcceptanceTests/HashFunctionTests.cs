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
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    [Collection("Acceptance tests")]
    public class HashFunctionTests : FlowtideAcceptanceBase
    {
        public HashFunctionTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task SelectWithXxHash128GuidString()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    xxhash128_guid_string(userkey) AS hash
                FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x =>
            {
                XxHash128 hash128 = new XxHash128();
                byte[] bytes = new byte[8];
                BinaryPrimitives.WriteInt64LittleEndian(bytes, x.UserKey);
                hash128.Append(bytes);
                var hash = hash128.GetHashAndReset();

                return new { hash = new Guid(hash).ToString() };
            }));
        }

        [Fact]
        public async Task SelectWithXxHash64()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    xxhash64(userkey) AS hash
                FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x =>
            {
                XxHash64 hash64 = new XxHash64();
                byte[] bytes = new byte[8];
                BinaryPrimitives.WriteInt64LittleEndian(bytes, x.UserKey);
                hash64.Append(bytes);
                var longHash = (long)hash64.GetCurrentHashAsUInt64();
                hash64.Reset();

                return new { hash = longHash };
            }));
        }
    }
}
