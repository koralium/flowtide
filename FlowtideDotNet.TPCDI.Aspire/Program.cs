using Projects;

var builder = DistributedApplication.CreateBuilder(args);

builder.AddProject<FlowtideDotNet_TpcDI>("project");

builder.Build().Run();
