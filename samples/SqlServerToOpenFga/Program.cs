using Microsoft.EntityFrameworkCore;
using SqlServerToOpenFga.Database;

var builder = WebApplication.CreateBuilder(args);

// Create a service collection that is used during startup to initialize the database
ServiceCollection startupCollection = new ServiceCollection();
startupCollection.AddDbContext<DataContext>(options =>
{
    options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection"));
});

var startupProvider = startupCollection.BuildServiceProvider();
var dbContext = startupProvider.GetRequiredService<DataContext>();
dbContext.Database.Migrate();
    


builder.Services.AddDbContext<DataContext>(options =>
{
    options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection"));
});

var app = builder.Build();

app.Run();