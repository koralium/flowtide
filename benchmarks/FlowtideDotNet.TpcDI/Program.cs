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
using FlowtideDotNet.Core;

var builder = WebApplication.CreateBuilder(args);


var datesql = File.ReadAllText("./dimdate.sql");
var taxratesql = File.ReadAllText("./taxrate.sql");
var tradetypesql = File.ReadAllText("./tradetype.sql");
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
var factcashbalancesql = File.ReadAllText("./factcashbalances.sql");
var factholdingssql = File.ReadAllText("./factholdings.sql");
var factwatchessql = File.ReadAllText("./factwatches.sql");
//var factmarkethistorysql = File.ReadAllText("./factmarkethistory.sql");


var combinedSql = string.Join(Environment.NewLine + Environment.NewLine,
    datesql,
    taxratesql,
    tradetypesql,
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
    dimtradesql,
    factcashbalancesql,
    factholdingssql,
    factwatchessql);
    //factmarkethistorysql);

var filesLocation = Files.Of.LocalDisk("./inputdata");

builder.Services.AddFlowtideStream("stream")
    .AddSqlTextAsPlan(combinedSql)
    .WriteCheckFailuresToLogger()
    .AddConnectors(c =>
    {
        c.AddCsvFileSource("dailymarket_raw", new CsvFileOptions()
        {
            CsvColumns = new List<string>()
            {
                "DM_DATE",
                "DM_S_SYMB",
                "DM_CLOSE",
                "DM_HIGH",
                "DM_LOW",
                "DM_VOL"
            },
            FileStorage = filesLocation,
            GetInitialFiles = () => Task.FromResult<IEnumerable<string>>(new List<string>()
            {
                "Batch1/DailyMarket.txt"
            }),
            Delimiter = "|",
            OutputSchema = new NamedStruct()
            {
                Names = new List<string>()
                {
                    "DM_DATE",
                    "DM_S_SYMB",
                    "DM_CLOSE",
                    "DM_HIGH",
                    "DM_LOW",
                    "DM_VOL"
                },
                Struct = new Struct()
                {
                    Types = new List<SubstraitBaseType>()
                    {
                        new TimestampType(),
                        new StringType(),
                        new Fp64Type(),
                        new Fp64Type(),
                        new Fp64Type(),
                        new Int64Type()
                    }
                }
            }
        });
        c.AddWatchHistoryData(filesLocation);
        c.AddHoldingHistoryData(filesLocation);
        c.AddCashTransactionData(filesLocation);
        c.AddTradeTypeData(filesLocation);
        c.AddTradeData(filesLocation);
        c.AddTradeHistoryData(filesLocation);
        c.AddStatusTypeData(filesLocation);
        c.AddIndustryData(filesLocation);
        c.AddFinwireData(filesLocation);
        c.AddHrData(filesLocation);
        c.AddTaxRateData(filesLocation);
        c.AddCustomerManagementData(filesLocation);
        c.AddDatesData(filesLocation);
        c.AddProspectData(filesLocation);

        c.AddCatalog("sink", (sink) =>
        {
            sink.AddDeltaLakeSink(new FlowtideDotNet.Connector.DeltaLake.DeltaLakeOptions()
            {
                StorageLocation = Files.Of.LocalDisk("./outputdata")
            });
            //sink.AddBlackholeSink("*");
        });
        
        c.AddConsoleSink("console");
        c.AddBlackholeSink("blackhole");
    })
    .AddStorage(s =>
    {
        //s.AddFasterKVFileSystemStorage("./tmpdir");
        s.AddTemporaryDevelopmentStorage();
    });

var app = builder.Build();

app.UseFlowtideUI("/stream");

app.Run();