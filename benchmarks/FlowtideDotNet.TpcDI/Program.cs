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

using FlowtideDotNet.DependencyInjection;
using FlowtideDotNet.Core.Sinks;
using FlowtideDotNet.Connector.Files;
using Stowage;
using FlowtideDotNet.Substrait.Type;
using FlowtideDotNet.Base;
using FlowtideDotNet.TpcDI.Extensions;
using FlowtideDotNet.AspNetCore.Extensions;

var builder = WebApplication.CreateBuilder(args);


var datesql = File.ReadAllText("./dimdate.sql");
var taxratesql = File.ReadAllText("./taxrate.sql");
var customerbasesql = File.ReadAllText("./customerbase.sql");
var prospectsql = File.ReadAllText("./prospect.sql");
var dimcustomersql = File.ReadAllText("./dimcustomer.sql");
var brokersql = File.ReadAllText("./dimbroker.sql");
var dimaccountsql = File.ReadAllText("./dimaccount.sql");
var finwiresql = File.ReadAllText("./finwire.sql");
var industrysql = File.ReadAllText("./industry.sql");
var statustypesql = File.ReadAllText("./statustype.sql");
var dimcompanysql = File.ReadAllText("./dimcompany.sql");
var dimsecuritysql = File.ReadAllText("./dimsecurity.sql");
var financialsql = File.ReadAllText("./financial.sql");
var dimtradesql = File.ReadAllText("./dimtrade.sql");


var combinedSql = string.Join(Environment.NewLine + Environment.NewLine,
    datesql,
    taxratesql,
    customerbasesql,
    prospectsql,
    dimcustomersql,
    brokersql,
    dimaccountsql,
    finwiresql,
    industrysql,
    statustypesql,
    dimcompanysql,
    dimsecuritysql,
    financialsql,
    dimtradesql);

var filesLocation = Files.Of.LocalDisk("./inputdata");

builder.Services.AddFlowtideStream("stream")
    .AddSqlTextAsPlan(combinedSql)
    .WriteCheckFailuresToLogger()
    .AddConnectors(c =>
    {
        c.AddCsvFileSource("trade_raw", new CsvFileOptions()
        {
            Delimiter = "|",
            CsvColumns = new List<string>()
            {
                "T_ID",
                "T_DTS",
                "T_ST_ID",
                "T_TT_ID",
                "T_IS_CASH",
                "T_S_SYMB",
                "T_QTY",
                "T_BID_PRICE",
                "T_CA_ID",
                "T_EXEC_NAME",
                "T_TRADE_PRICE",
                "T_CHRG",
                "T_COMM",
                "T_TAX"
            },
            FileStorage = filesLocation,
            GetInitialFiles = () => Task.FromResult<IEnumerable<string>>(new List<string>()
            {
                "Batch1/Trade.txt"
            }),
            OutputSchema = new NamedStruct()
            {
                Names = new List<string>()
                {
                    "T_ID",
                    "T_DTS",
                    "T_ST_ID",
                    "T_TT_ID",
                    "T_IS_CASH",
                    "T_S_SYMB",
                    "T_QTY",
                    "T_BID_PRICE",
                    "T_CA_ID",
                    "T_EXEC_NAME",
                    "T_TRADE_PRICE",
                    "T_CHRG",
                    "T_COMM",
                    "T_TAX"
                },
                Struct = new Struct()
                {
                    Types = new List<SubstraitBaseType>()
                    {
                        new Int64Type(),
                        new TimestampType(),
                        new StringType(),
                        new StringType(),
                        new StringType(),
                        new StringType(),
                        new Int64Type(),
                        new DecimalType(),
                        new StringType(),
                        new StringType(),
                        new DecimalType(),
                        new DecimalType(),
                        new DecimalType(),
                        new DecimalType(),
                    }
                }
            }
        });
        c.AddTradeHistoryData(filesLocation);
        c.AddStatusTypeData(filesLocation);
        c.AddIndustryData(filesLocation);
        c.AddFinwireData(filesLocation);
        c.AddHrData(filesLocation);
        c.AddTaxRateData(filesLocation);
        c.AddCustomerManagementData(filesLocation);
        c.AddDatesData(filesLocation);
        c.AddProspectData(filesLocation);

        c.AddConsoleSink("console");
        c.AddBlackholeSink("blackhole");
    })
    .AddStorage(s =>
    {
        s.AddTemporaryDevelopmentStorage();
    });

var app = builder.Build();

app.UseFlowtideUI("/stream");

app.Run();