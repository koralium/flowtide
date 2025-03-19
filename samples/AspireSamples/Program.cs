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

using AspireSamples.DeltaLakeSourceSample;
using AspireSamples.ElasticsearchExample;
using AspireSamples.MongoDbToConsole;
using Microsoft.Extensions.Configuration;
using Spectre.Console;
using SqlServerToSqlServerAspire.SqlServerToSqlServer;

var builder = DistributedApplication.CreateBuilder(args);

string? sample = builder.Configuration.GetValue<string?>("sample");
if (sample == null)
{
    sample = AnsiConsole.Prompt(
    new SelectionPrompt<string>()
        .Title("Select sample:")
        .PageSize(10)
        .AddChoices(new[] {
            "SqlServer-To-SqlServer",
            "MongoDB-To-Console",
            "DeltaLake-Source",
            "DeltaLake-Source, Replay history",
            "SqlServer-To-Elasticsearch"
        }));
}

switch (sample)
{
    case "SqlServer-To-SqlServer":
        SqlServerToSqlServerSample.RunSample(builder);
        break;
    case "MongoDB-To-Console":
        MongoDbToConsoleStartup.RunSample(builder);
        break;
    case "DeltaLake-Source":
        DeltaLakeSourceStartup.RunSample(builder, false);
        break;
    case "DeltaLake-Source, Replay history":
        DeltaLakeSourceStartup.RunSample(builder, true);
        break;
    case "SqlServer-To-Elasticsearch":
        ElasticsearchExampleStartup.RunSample(builder);
        break;
}