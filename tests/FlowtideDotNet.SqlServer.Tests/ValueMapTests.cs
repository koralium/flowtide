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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Tests.SqlServer;
using System.Data.Common;

namespace FlowtideDotNet.SqlServer.Tests
{
    public class ValueMapTests
    {
        private class TestColumn : DbColumn
        {
            public TestColumn(Type dataType)
            {
                DataType = dataType;
            }
        }

        [Fact]
        public void TestTimestampToDateTimeIsUtc()
        {
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new TimestampTzValue(new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.FromHours(2)))
            };
            var map = SqlServerUtils.GetColumnToDataTableValueMap(new TestColumn(typeof(DateTime)), new DataValueContainer(), 0);

            Assert.Equal(new DateTime(2024, 1, 1, 10, 0, 0), (DateTime)map(new EventBatchData([column]), 0)!);
        }
    }
}
