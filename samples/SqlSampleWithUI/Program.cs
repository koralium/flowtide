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

using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Base;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Sinks;
using FlowtideDotNet.DependencyInjection;
using SqlSampleWithUI;

var builder = WebApplication.CreateBuilder(args);

// Map flowtide pause options to enable pausing and resuming from configuration
builder.Services.AddOptions<FlowtidePauseOptions>()
    .Bind(builder.Configuration.GetSection("flowtide"));

var sqlText = @"
CREATE TABLE testtable (
  val any,
val1 any,
val2 any,
val3 any,
val4 any,
val5 any
);

CREATE TABLE other (
  val any,
val1 any,
val2 any,
val3 any,
val4 any,
val5 any
);

INSERT INTO output
SELECT t.val, o.val, t.val1, t.val2, t.val3, t.val4, t.val5, o.val1, o.val2, o.val3, o.val4, o.val5 FROM testtable t
INNER JOIN other o
ON t.val = o.val;
";

builder.Services.AddFlowtideStream("test")
.AddSqlTextAsPlan(sqlText)
.AddVersioningFromString("1.0.3")
.AddConnectors((connectorManager) =>
{
    connectorManager.AddSource(new DummyReadFactory("*"));
    connectorManager.AddBlackholeSink("*");
})
.AddStorage(b =>
{
    b.AddFileStorage("./stateData")
    .OldStreamVersionsRetention(0);

    b.MaxProcessMemory = 2L * 1024 * 1024 * 1024;
    b.MinPageCount = 0;
});

builder.Services.AddCors();

builder.Services.AddHealthChecks()
    .AddFlowtideCheck();

var app = builder.Build();
app.UseCors(b =>
{
    b.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader();
});

app.UseHealthChecks("/health");
app.UseFlowtideUI("/");

app.Run();