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
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.TpcDI.Extensions;
using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Core;

var builder = WebApplication.CreateBuilder(args);


var datesql = File.ReadAllText("./sql/dimensions/dimdate.sql");
var taxratesql = File.ReadAllText("./sql/reference/taxrate.sql");
var tradetypesql = File.ReadAllText("./sql/reference/tradetype.sql");
var customerbasesql = File.ReadAllText("./sql/preprocess/customerbase.sql");
var prospectsql = File.ReadAllText("./sql/facts/prospect.sql");
var dimcustomersql = File.ReadAllText("./sql/dimensions/dimcustomer.sql");
var brokersql = File.ReadAllText("./sql/dimensions/dimbroker.sql");
var dimaccountsql = File.ReadAllText("./sql/dimensions/dimaccount.sql");
var finwiresql = File.ReadAllText("./sql/preprocess/finwire.sql");
var industrysql = File.ReadAllText("./sql/reference/industry.sql");
var statustypesql = File.ReadAllText("./sql/reference/statustype.sql");
var dimcompanysql = File.ReadAllText("./sql/dimensions/dimcompany.sql");
var dimsecuritysql = File.ReadAllText("./sql/dimensions/dimsecurity.sql");
var financialsql = File.ReadAllText("./sql/reference/financial.sql");
var dimtradesql = File.ReadAllText("./sql/dimensions/dimtrade.sql");
var factcashbalancesql = File.ReadAllText("./sql/facts/factcashbalances.sql");
var factholdingssql = File.ReadAllText("./sql/facts/factholdings.sql");
var factwatchessql = File.ReadAllText("./sql/facts/factwatches.sql");
//var factmarkethistorysql = File.ReadAllText("./sql/facts/factmarkethistory.sql");

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
    //.WriteCheckFailuresToLogger()
    .AddConnectors(c =>
    {
        c.AddDailyMarketData(filesLocation);
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
            //sink.AddDeltaLakeSink(new FlowtideDotNet.Connector.DeltaLake.DeltaLakeOptions()
            //{
            //    StorageLocation = Files.Of.LocalDisk("./outputdata")
            //});
            sink.AddBlackholeSink("*");
        });
        
        c.AddConsoleSink("console");
        c.AddBlackholeSink("blackhole");
    })
    .AddStorage(s =>
    {
        s.AddFasterKVFileSystemStorage("./tmpdir");
        //s.MaxProcessMemory = 8 * 1024 * 1024 * 1024L; // 8GB
        s.MinPageCount = 100_000;
        s.MaxPageCount = 150_000;

        //s.AddTemporaryDevelopmentStorage();
    });

var app = builder.Build();

app.UseFlowtideUI("/stream");

app.MapGet("/next_batch", async ([FromKeyedServices("stream")] FlowtideDotNet.Base.Engine.DataflowStream services) =>
{
    await services.CallTrigger("delta_load", default);
    return Results.Ok();
});

app.Run();