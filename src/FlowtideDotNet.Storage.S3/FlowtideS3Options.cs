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

using Amazon;
using Amazon.Runtime;
using Amazon.S3;

namespace FlowtideDotNet.Storage.S3
{
    public class FlowtideS3Options
    {
        /// <summary>
        /// Gets or sets the name of the S3 bucket where the reservoir data will be stored.
        /// This property is required.
        /// </summary>
        public string? BucketName { get; set; }

        /// <summary>
        /// Gets or sets the Access Key ID used for authentication.
        /// </summary>
        public string? AccessKey { get; set; }

        /// <summary>
        /// Gets or sets the Secret Access Key used for authentication.
        /// </summary>
        public string? SecretKey { get; set; }

        /// <summary>
        /// Gets or sets the Session Token used for temporary credentials (e.g., via AWS STS).
        /// </summary>
        public string? SessionToken { get; set; }

        /// <summary>
        /// Gets or sets a custom service endpoint URL. 
        /// This is required when connecting to non-AWS S3-compatible providers such as MinIO, Wasabi, or Cloudflare R2.
        /// </summary>
        public string? Endpoint { get; set; }

        /// <summary>
        /// Gets or sets the AWS region to connect to (e.g., "eu-west-1").
        /// If <see cref="Endpoint"/> is set for a custom provider, this acts as the signing region.
        /// </summary>
        public string? Region { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to force path-style URLs for S3 objects 
        /// (e.g., "https://s3.amazonaws.com/bucketName" instead of "https://bucketName.s3.amazonaws.com").
        /// This must typically be set to true when using local emulators like MinIO or LocalStack.
        /// </summary>
        public bool ForcePathStyle { get; set; }

        /// <summary>
        /// Gets or sets a pre-configured AWSCredentials object. 
        /// If provided, this will override the <see cref="AccessKey"/>, <see cref="SecretKey"/>, and <see cref="SessionToken"/> properties.
        /// </summary>
        public AWSCredentials? Credentials { get; set; }

        /// <summary>
        /// Gets or sets a custom configuration for the S3 client. 
        /// If provided, properties like <see cref="Endpoint"/>, <see cref="Region"/>, and <see cref="ForcePathStyle"/> 
        /// defined on this options class will be ignored in favor of this configuration object.
        /// </summary>
        public AmazonS3Config? ClientConfig { get; set; }

        /// <summary>
        /// Gets or sets a factory delegate used to create instances of the <see cref="IAmazonS3"/> client.
        /// If provided, this function takes full priority and bypasses all other credential and configuration properties.
        /// </summary>
        public Func<IAmazonS3>? ClientFactory { get; set; }

        /// <summary>
        /// Gets or sets an optional prefix or sub-directory path within the bucket. 
        /// All reservoir files will be stored under this logical path.
        /// </summary>
        public string? DirectoryPath { get; set; }

        /// <summary>
        /// Gets or sets the path to the local directory used for caching data.
        /// If not set, a temporary folder will be allocated automatically.
        /// </summary>
        public string? LocalCacheDirectory { get; set; }

        internal IAmazonS3 GetClient()
        {
            // 1. If a custom client factory is provided, use it directly.
            if (ClientFactory != null)
            {
                return ClientFactory();
            }

            if (string.IsNullOrWhiteSpace(BucketName))
            {
                throw new InvalidOperationException("A valid BucketName must be provided to initialize the S3 storage provider.");
            }

            // 2. Resolve Credentials
            AWSCredentials? credentialsToUse = Credentials;
            if (credentialsToUse == null && !string.IsNullOrWhiteSpace(AccessKey) && !string.IsNullOrWhiteSpace(SecretKey))
            {
                if (!string.IsNullOrWhiteSpace(SessionToken))
                {
                    credentialsToUse = new SessionAWSCredentials(AccessKey, SecretKey, SessionToken);
                }
                else
                {
                    credentialsToUse = new BasicAWSCredentials(AccessKey, SecretKey);
                }
            }

            // 3. Resolve Configuration
            AmazonS3Config configToUse = ClientConfig ?? new AmazonS3Config();

            // Only apply our shorthand properties if the user hasn't supplied a full custom config
            if (ClientConfig == null)
            {
                configToUse.ForcePathStyle = ForcePathStyle;

                if (!string.IsNullOrWhiteSpace(Endpoint))
                {
                    configToUse.ServiceURL = Endpoint;
                }

                if (!string.IsNullOrWhiteSpace(Region))
                {
                    // If an endpoint is set, setting the Region directly might throw in the SDK. 
                    // AuthenticationRegion is safer for custom providers.
                    if (!string.IsNullOrWhiteSpace(Endpoint))
                    {
                        configToUse.AuthenticationRegion = Region;
                    }
                    else
                    {
                        configToUse.RegionEndpoint = RegionEndpoint.GetBySystemName(Region);
                    }
                }
            }

            // 4. Create and return the client
            // If credentialsToUse is null, the AWS SDK automatically falls back to environment variables or IAM roles.
            if (credentialsToUse != null)
            {
                return new AmazonS3Client(credentialsToUse, configToUse);
            }
            else
            {
                return new AmazonS3Client(configToUse);
            }
        }
    }
}
