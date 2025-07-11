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

using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;

namespace FlowtideDotNet.Core.Tests.SmokeTests
{
    public static class TpchData
    {
        private static List<LineItem>? _lineItems;
        private static List<Order>? _orders;

        /// <summary>
        /// Extra custom table to test string joins
        /// </summary>
        private static List<Shipmode>? _shipmodes;

        private static List<T> LoadData<T>(string path)
        {
            using var reader = new StreamReader(path);
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                PrepareHeaderForMatch = args => args.Header.ToLower(),
            };


            using var csv = new CsvReader(reader, config);
            return csv.GetRecords<T>().ToList();
        }

        internal static DateTime FixDate(DateTime time)
        {
            var utc = time.ToUniversalTime();
            TimeSpan diff = time.Subtract(utc);
            return utc.Add(diff);
        }


        public static List<LineItem> GetLineItems()
        {
            if (_lineItems != null)
            {
                return _lineItems;
            }
            _lineItems = LoadData<LineItem>("./SmokeTests/lineitem.csv");
            _lineItems.ForEach(x =>
            {
                x.Commitdate = FixDate(x.Commitdate);
                x.Receiptdate = FixDate(x.Receiptdate);
                x.Shipdate = FixDate(x.Shipdate);
            });
            return _lineItems;
        }

        public static List<Order> GetOrders()
        {
            if (_orders != null)
            {
                return _orders;
            }
            _orders = LoadData<Order>("./SmokeTests/orders.csv");
            return _orders;
        }

        public static List<Shipmode> GetShipmodes()
        {
            if (_shipmodes != null)
            {
                return _shipmodes;
            }
            _shipmodes = LoadData<Shipmode>("./SmokeTests/shipmode.csv");
            return _shipmodes;
        }

        public static async Task InsertIntoDbContext(TpchDbContext dbContext)
        {
            var lineItems = GetLineItems();
            dbContext.LineItems.AddRange(lineItems);

            var orders = GetOrders();
            dbContext.Orders.AddRange(orders);

            var shipmodes = GetShipmodes();
            dbContext.Shipmodes.AddRange(shipmodes);

            await dbContext.SaveChangesAsync();
        }

    }
}
