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

using System.IO.Compression;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    public class CompressionTests : FlowtideAcceptanceBase
    {
        public CompressionTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task TestZLipCompression()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                INNER JOIN users u
                ON o.userkey = u.userkey",
                stateSerializeOptions: new Storage.StateSerializeOptions()
                {
                    CompressFunc = (stream) =>
                    {
                        return new System.IO.Compression.ZLibStream(stream, CompressionMode.Compress);
                    },
                    DecompressFunc = (stream) =>
                    {
                        return new System.IO.Compression.ZLibStream(stream, CompressionMode.Decompress);
                    }
                });
            await WaitForUpdate();
            await Crash();
            GenerateData();
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Join(Users, x => x.UserKey, x => x.UserKey, (l, r) => new { l.OrderKey, r.FirstName, r.LastName }));
        }
    }
}
