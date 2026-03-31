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

using Azure;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.AzureBlobs.Internal
{
    internal class HttpBlobClient
    {
        private readonly BlobContainerClient _containerClient;
        private HttpPipeline _httpPipeline;

        public HttpBlobClient(BlobContainerClient containerClient)
        {
            // These should change later to unsafe accessors in .net 10
            var clientConfigField = typeof(BlobContainerClient).GetField("_clientConfiguration", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            
            if (clientConfigField == null)
            {
                throw new InvalidOperationException("Unable to access client configuration from BlobContainerClient. HttpBlobClient requires access to the client's HttpPipeline.");
            }

            var clientConfig = clientConfigField.GetValue(containerClient);

            if (clientConfig == null)
            {
                throw new InvalidOperationException("Client configuration is null. HttpBlobClient requires access to the client's HttpPipeline.");
            }

            var clientConfigType = clientConfig.GetType();
            clientConfigType.GetProperties();
            var pipelineProperty = clientConfigType.GetProperty("Pipeline", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.GetProperty | System.Reflection.BindingFlags.SetProperty);

            if (pipelineProperty == null)
            {
                throw new InvalidOperationException("Unable to access HttpPipeline from client configuration. HttpBlobClient requires access to the client's HttpPipeline.");
            }

            var pipelineObj = pipelineProperty.GetValue(clientConfig);
            var pipeline = (HttpPipeline?)pipelineObj;

            if (pipeline == null)
            {
                throw new InvalidOperationException("HttpPipeline is null. HttpBlobClient requires access to the client's HttpPipeline.");
            }
            _httpPipeline = pipeline;
            this._containerClient = containerClient;
        }

        public async Task PutBlob(string blobName, ReadOnlySequence<byte> data, CancellationToken cancellationToken) 
        {
            using var message = _httpPipeline.CreateMessage();
            var request = message.Request;

            var blobUri = new BlobUriBuilder(_containerClient.Uri) { BlobName = blobName }.ToUri();

            request.Method = RequestMethod.Put;
            request.Uri.Reset(blobUri);

            request.Headers.Add("x-ms-blob-type", "BlockBlob");
            request.Headers.Add("x-ms-version", "2024-05-04");
            request.Headers.Add("x-ms-date", DateTimeOffset.UtcNow.ToString("R"));

            request.Content = RequestContent.Create(data);

            await _httpPipeline.SendAsync(message, cancellationToken).ConfigureAwait(false);

            if (message.Response.Status < 200 || message.Response.Status >= 300)
            {
                throw new RequestFailedException(message.Response);
            }
        }
    }
}
