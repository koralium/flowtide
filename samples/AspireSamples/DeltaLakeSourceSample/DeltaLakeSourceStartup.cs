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

using AspireSamples.DataMigration;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using Projects;
using Stowage;

namespace AspireSamples.DeltaLakeSourceSample
{
    internal static class DeltaLakeSourceStartup
    {
        private static (Uri endpoint, string accountName, string sharedkey) GetUriAndSharedKeyFromBlobConnectionString(string connectionString)
        {
            var parts = connectionString.Split(';');
            var endpoint = parts.First(p => p.StartsWith("BlobEndpoint=")).Substring("BlobEndpoint=".Length);
            var sharedkey = parts.First(p => p.StartsWith("AccountKey=")).Substring("AccountKey=".Length);
            var accountName = parts.First(p => p.StartsWith("AccountName=")).Substring("AccountName=".Length);
            return (new Uri(endpoint), accountName, sharedkey);
        }

        public static void RunSample(IDistributedApplicationBuilder builder, bool replayHistory)
        {
            var azureStorage = builder.AddAzureStorage("storage")
                .RunAsEmulator();

            var blobs = azureStorage
                .AddBlobs("lake");

            var persistenceBlob = azureStorage
                .AddBlobs("persistence");

            DataGenerator dataGenerator = new DataGenerator();

            var dataInsert = DataInsertResource.AddDataInsert(builder, "data-copy",
                async (logger, statusUpdate, token) =>

                {
                    statusUpdate("Copying data to Azure Blob Storage");
                    var blob = blobs;
                    var blobResource = blob.Resource;

                    var connStr = await blobResource.ConnectionStringExpression.GetValueAsync(default);
                    var testData = Files.Of.LocalDisk("../../tests/FlowtideDotNet.Connector.DeltaLake.Tests/testdata");
                    var lsResult = await testData.Ls("./", recurse: true);

                    var (blobUri, accountName, sharedKey) = GetUriAndSharedKeyFromBlobConnectionString(connStr!);

                    var azureFile = Files.Of.AzureBlobStorage(blobUri, accountName, sharedKey);

                    var blobServiceClient = new BlobServiceClient(connStr);
                    var container = blobServiceClient.CreateBlobContainer("datalake");
                    container.Value.CreateIfNotExists();

                    foreach (var file in lsResult)
                    {
                        if (file.Path.IsFolder)
                        {
                            continue;
                        }
                        if (file.Name.StartsWith("."))
                        {
                            continue;
                        }
                        var fullPath = file.Path.Full;

                        logger.LogInformation($"Copying {fullPath} into blob storage");
                        fullPath = "/datalake" + fullPath;
                        var filePath = new IOPath(fullPath);
                        using var localFile = await testData.OpenRead(file.Path);

                        using var remoteFile = await azureFile.OpenWrite(filePath);
                        await localFile!.CopyToAsync(remoteFile);
                        await remoteFile.FlushAsync();
                    }
                },
                (logger, token) => Task.CompletedTask)
                .WaitFor(blobs);

            var stream = builder.AddProject<DeltaLakeSourceUsage>("stream")
                .WithHttpHealthCheck("/health")
                .WithReference(blobs)
                .WithReference(persistenceBlob)
                .WithEnvironment("replay", replayHistory.ToString())
                .WaitFor(dataInsert);

            builder.Build().Run();
        }
    }
}
