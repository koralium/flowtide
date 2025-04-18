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

using FlowtideDotNet.Connector.Files;
using FlowtideDotNet.Core;
using FlowtideDotNet.Substrait.Type;
using Stowage;

namespace FlowtideDotNet.TpcDI.Extensions
{
    public static class DateExtensions
    {
        public static IConnectorManager AddDatesData(this IConnectorManager connectorManager, IFileStorage filesLocation)
        {
            connectorManager.AddCsvFileSource("dates_raw", new CsvFileOptions()
            {
                CsvColumns = new List<string>()
                {
                    "SK_DateID",
                    "DateValue",
                    "DateDesc",
                    "CalendarYearID",
                    "CalendarYearDesc",
                    "CalendarQtrID",
                    "CalendarQtrDesc",
                    "CalendarMonthID",
                    "CalendarMonthDesc",
                    "CalendarWeekID",
                    "CalendarWeekDesc",
                    "DayOfWeekNum",
                    "DayOfWeekDesc",
                    "FiscalYearID",
                    "FiscalYearDesc",
                    "FiscalQtrID",
                    "FiscalQtrDesc",
                    "HolidayFlag"
                },
                FileStorage = filesLocation,
                GetInitialFiles = () => Task.FromResult<IEnumerable<string>>(new List<string>()
                {
                    "Batch1/Date.txt"
                }),
                Delimiter = "|",
                OutputSchema = new NamedStruct()
                {
                    Names = new List<string>()
                    {
                        "SK_DateID",
                        "DateValue",
                        "DateDesc",
                        "CalendarYearID",
                        "CalendarYearDesc",
                        "CalendarQtrID",
                        "CalendarQtrDesc",
                        "CalendarMonthID",
                        "CalendarMonthDesc",
                        "CalendarWeekID",
                        "CalendarWeekDesc",
                        "DayOfWeekNum",
                        "DayOfWeekDesc",
                        "FiscalYearID",
                        "FiscalYearDesc",
                        "FiscalQtrID",
                        "FiscalQtrDesc",
                        "HolidayFlag"
                    },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>()
                        {
                            new Int64Type(),
                            new StringType(),
                            new StringType(),
                            new Int64Type(),
                            new StringType(),
                            new Int64Type(),
                            new StringType(),
                            new Int64Type(),
                            new StringType(),
                            new Int64Type(),
                            new StringType(),
                            new Int64Type(),
                            new StringType(),
                            new Int64Type(),
                            new StringType(),
                            new Int64Type(),
                            new StringType(),
                            new BoolType()
                        }
                    }
                }
            });

            return connectorManager;
        }
    }
}
