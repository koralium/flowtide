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

using FlowtideDotNet.Connector.MongoDB.Internal;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.Memory;
using MongoDB.Bson;

namespace FlowtideDotNet.Connector.MongoDB.Tests
{
    public class BsonConversionTests
    {
        [Fact]
        public void TestTimestampToBsonIsUtc()
        {
            var value = new TimestampTzValue(new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.FromHours(2)));
            var bson = ColumnsToBsonDoc.ValueToBson(value);

            Assert.Equal(new DateTimeOffset(2024, 1, 1, 10, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds(), bson.AsBsonDateTime.MillisecondsSinceEpoch);
        }

        [Fact]
        public void TestNestedBsonTimestampIsInt64()
        {
            var document = new BsonDocument("v", new BsonDocument("t", new BsonTimestamp(1700000000, 1)));
            var column = new Column(GlobalMemoryManager.Instance);
            new BsonDocToColumn("v").AddToColumn(column, document);

            var map = column.GetValueAt(0, default).AsMap;
            Assert.Equal(1700000000, map.GetValueAt(0).AsLong);
        }
    }
}
