#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
USER app
WORKDIR /app
EXPOSE 8080
EXPOSE 8081

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Directory.Build.props", "."]
COPY ["samples/SqlServerToOpenFga/SqlServerToOpenFga.csproj", "samples/SqlServerToOpenFga/"]
COPY ["src/FlowtideDotNet.AspNetCore/FlowtideDotNet.AspNetCore.csproj", "src/FlowtideDotNet.AspNetCore/"]
COPY ["src/FlowtideDotNet.Core/FlowtideDotNet.Core.csproj", "src/FlowtideDotNet.Core/"]
COPY ["src/FlowtideDotNet.Base/FlowtideDotNet.Base.csproj", "src/FlowtideDotNet.Base/"]
COPY ["src/FlowtideDotNet.Storage/FlowtideDotNet.Storage.csproj", "src/FlowtideDotNet.Storage/"]
COPY ["src/FlowtideDotNet.Substrait/FlowtideDotNet.Substrait.csproj", "src/FlowtideDotNet.Substrait/"]
COPY ["src/FlowtideDotNet.Connector.OpenFGA/FlowtideDotNet.Connector.OpenFGA.csproj", "src/FlowtideDotNet.Connector.OpenFGA/"]
COPY ["src/FlowtideDotNet.Zanzibar/FlowtideDotNet.Zanzibar.csproj", "src/FlowtideDotNet.Zanzibar/"]
COPY ["src/FlowtideDotNet.Connector.SqlServer/FlowtideDotNet.Connector.SqlServer.csproj", "src/FlowtideDotNet.Connector.SqlServer/"]
RUN dotnet restore "./samples/SqlServerToOpenFga/./SqlServerToOpenFga.csproj"
COPY . .
WORKDIR "/src/samples/SqlServerToOpenFga"
RUN dotnet build "./SqlServerToOpenFga.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./SqlServerToOpenFga.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "SqlServerToOpenFga.dll"]