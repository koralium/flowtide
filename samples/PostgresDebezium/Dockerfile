#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:7.0 AS base
WORKDIR /app
EXPOSE 80

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["samples/PostgresDebezium/PostgresDebezium.csproj", "samples/PostgresDebezium/"]
COPY ["src/FlowtideDotNet.AspNetCore/FlowtideDotNet.AspNetCore.csproj", "src/FlowtideDotNet.AspNetCore/"]
COPY ["src/FlowtideDotNet.Core/FlowtideDotNet.Core.csproj", "src/FlowtideDotNet.Core/"]
COPY ["src/FlowtideDotNet.Base/FlowtideDotNet.Base.csproj", "src/FlowtideDotNet.Base/"]
COPY ["src/FlowtideDotNet.Storage/FlowtideDotNet.Storage.csproj", "src/FlowtideDotNet.Storage/"]
COPY ["src/FlowtideDotNet.Substrait/FlowtideDotNet.Substrait.csproj", "src/FlowtideDotNet.Substrait/"]
COPY ["src/FlowtideDotNet.Connector.Kafka/FlowtideDotNet.Connector.Kafka.csproj", "src/FlowtideDotNet.Connector.Kafka/"]
RUN dotnet restore "./samples/PostgresDebezium/./PostgresDebezium.csproj"
COPY . .
WORKDIR "/src/samples/PostgresDebezium"
RUN dotnet build "./PostgresDebezium.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./PostgresDebezium.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "PostgresDebezium.dll"]