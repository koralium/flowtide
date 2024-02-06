#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:7.0 AS base
WORKDIR /app
EXPOSE 80

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["samples/OpenFgaSqlServerToMongoDb/OpenFgaSqlServerToMongoDb.csproj", "samples/OpenFgaSqlServerToMongoDb/"]
COPY ["src/FlowtideDotNet.AspNetCore/FlowtideDotNet.AspNetCore.csproj", "src/FlowtideDotNet.AspNetCore/"]
COPY ["src/FlowtideDotNet.Core/FlowtideDotNet.Core.csproj", "src/FlowtideDotNet.Core/"]
COPY ["src/FlowtideDotNet.Base/FlowtideDotNet.Base.csproj", "src/FlowtideDotNet.Base/"]
COPY ["src/FlowtideDotNet.Storage/FlowtideDotNet.Storage.csproj", "src/FlowtideDotNet.Storage/"]
COPY ["src/FlowtideDotNet.Substrait/FlowtideDotNet.Substrait.csproj", "src/FlowtideDotNet.Substrait/"]
COPY ["src/FlowtideDotNet.Connector.MongoDB/FlowtideDotNet.Connector.MongoDB.csproj", "src/FlowtideDotNet.Connector.MongoDB/"]
COPY ["src/FlowtideDotNet.Connector.OpenFGA/FlowtideDotNet.Connector.OpenFGA.csproj", "src/FlowtideDotNet.Connector.OpenFGA/"]
COPY ["src/FlowtideDotNet.Connector.SqlServer/FlowtideDotNet.Connector.SqlServer.csproj", "src/FlowtideDotNet.Connector.SqlServer/"]
RUN dotnet restore "./samples/OpenFgaSqlServerToMongoDb/./OpenFgaSqlServerToMongoDb.csproj"
COPY . .
WORKDIR "/src/samples/OpenFgaSqlServerToMongoDb"
RUN dotnet build "./OpenFgaSqlServerToMongoDb.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./OpenFgaSqlServerToMongoDb.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "OpenFgaSqlServerToMongoDb.dll"]