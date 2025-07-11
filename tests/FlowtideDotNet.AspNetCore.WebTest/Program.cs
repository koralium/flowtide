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
using FlowtideDotNet.AspNetCore.WebTest;
using FlowtideDotNet.Core;
using FlowtideDotNet.DependencyInjection;
using FlowtideDotNet.Substrait;

var builder = WebApplication.CreateBuilder(args);

SubstraitDeserializer substraitDeserializer = new SubstraitDeserializer();
var plan = substraitDeserializer.Deserialize(File.ReadAllText("queryplan.json"));

var connectorManager = new ConnectorManager();
connectorManager.AddSource(new DummyReadFactory("*"));
connectorManager.AddSink(new DummyWriteFactory("*"));

PlanModifier planModifier = new PlanModifier();
planModifier.AddRootPlan(plan);
// Test is running a query plan that uses deprecated method.
#pragma warning disable CS0618 // Type or member is obsolete
planModifier.WriteToTable("dummy");
#pragma warning restore CS0618 // Type or member is obsolete
plan = planModifier.Modify();

builder.Services.AddFlowtideStream("stream")
    .AddPlan(plan)
    .AddConnectors(c =>
    {
        c.AddSource(new DummyReadFactory("*"));
        c.AddSink(new DummyWriteFactory("*"));
    })
    .AddStorage(s =>
    {
        s.AddTemporaryDevelopmentStorage();
    });

// Add services to the container.
builder.Services.AddRazorPages();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}




app.UseHttpsRedirection();
app.UseStaticFiles();

app.UseRouting();
app.UseFlowtideUI("/");

app.UseAuthorization();

app.MapRazorPages();

app.Run();
