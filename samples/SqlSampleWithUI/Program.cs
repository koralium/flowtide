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
  val any
);

CREATE TABLE other (
  val any
);

INSERT INTO output
SELECT map('a', t.val) FROM testtable t
LEFT JOIN other o
ON t.val = o.val;
";

builder.Services.AddFlowtideStream("test")
.AddSqlTextAsPlan(sqlText)
.AddConnectors((connectorManager) =>
{
    connectorManager.AddSource(new DummyReadFactory("*"));
    connectorManager.AddConsoleSink("*");
})
.AddStorage(b =>
{
    b.AddTemporaryDevelopmentStorage();
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