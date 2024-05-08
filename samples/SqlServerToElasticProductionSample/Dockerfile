#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
USER app
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Directory.Build.props", "."]
COPY ["samples/SqlServerToElasticProductionSample/SqlServerToElasticProductionSample.csproj", "samples/SqlServerToElasticProductionSample/"]
COPY ["src/FlowtideDotNet.AspNetCore/FlowtideDotNet.AspNetCore.csproj", "src/FlowtideDotNet.AspNetCore/"]
COPY ["src/FlowtideDotNet.Core/FlowtideDotNet.Core.csproj", "src/FlowtideDotNet.Core/"]
COPY ["src/FlowtideDotNet.Base/FlowtideDotNet.Base.csproj", "src/FlowtideDotNet.Base/"]
COPY ["src/FlowtideDotNet.Storage/FlowtideDotNet.Storage.csproj", "src/FlowtideDotNet.Storage/"]
COPY ["src/FlowtideDotNet.Substrait/FlowtideDotNet.Substrait.csproj", "src/FlowtideDotNet.Substrait/"]
COPY ["src/FlowtideDotNet.Connector.ElasticSearch/FlowtideDotNet.Connector.ElasticSearch.csproj", "src/FlowtideDotNet.Connector.ElasticSearch/"]
COPY ["src/FlowtideDotNet.Connector.SqlServer/FlowtideDotNet.Connector.SqlServer.csproj", "src/FlowtideDotNet.Connector.SqlServer/"]
RUN dotnet restore "./samples/SqlServerToElasticProductionSample/./SqlServerToElasticProductionSample.csproj"
COPY . .
WORKDIR "/src/samples/SqlServerToElasticProductionSample"
RUN dotnet build "./SqlServerToElasticProductionSample.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./SqlServerToElasticProductionSample.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "SqlServerToElasticProductionSample.dll"]