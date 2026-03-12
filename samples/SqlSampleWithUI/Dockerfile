# See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

# This stage is used when running from VS in fast mode (Default for Debug configuration)
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080


# This stage is used to build the service project
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Directory.Packages.props", "."]
COPY ["Directory.Build.props", "."]
COPY ["nuget.config", "."]
COPY ["samples/SqlSampleWithUI/SqlSampleWithUI.csproj", "samples/SqlSampleWithUI/"]
COPY ["src/FlowtideDotNet.AspNetCore/FlowtideDotNet.AspNetCore.csproj", "src/FlowtideDotNet.AspNetCore/"]
COPY ["src/FlowtideDotNet.Core/FlowtideDotNet.Core.csproj", "src/FlowtideDotNet.Core/"]
COPY ["src/FlowtideDotNet.Base/FlowtideDotNet.Base.csproj", "src/FlowtideDotNet.Base/"]
COPY ["src/FlowtideDotNet.Storage/FlowtideDotNet.Storage.csproj", "src/FlowtideDotNet.Storage/"]
COPY ["src/FlowtideDotNet.Substrait/FlowtideDotNet.Substrait.csproj", "src/FlowtideDotNet.Substrait/"]
COPY ["src/FlowtideDotNet.DependencyInjection/FlowtideDotNet.DependencyInjection.csproj", "src/FlowtideDotNet.DependencyInjection/"]
COPY ["src/FlowtideDotNet.Connector.SqlServer/FlowtideDotNet.Connector.SqlServer.csproj", "src/FlowtideDotNet.Connector.SqlServer/"]
RUN dotnet restore "./samples/SqlSampleWithUI/SqlSampleWithUI.csproj"
COPY . .
WORKDIR "/src/samples/SqlSampleWithUI"
RUN dotnet build "./SqlSampleWithUI.csproj" -c $BUILD_CONFIGURATION -o /app/build

# This stage is used to publish the service project to be copied to the final stage
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./SqlSampleWithUI.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

# This stage is used in production or when running from VS in regular mode (Default when not using the Debug configuration)
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "SqlSampleWithUI.dll"]