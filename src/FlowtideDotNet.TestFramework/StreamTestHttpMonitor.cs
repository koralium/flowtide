using FlowtideDotNet.AspNetCore.Testing;
using System.Net.Http.Json;

namespace FlowtideDotNet.TestFramework
{
    /// <summary>
    /// Monitors a stream for updates by using the testInformation endpoint
    /// </summary>
    public class StreamTestHttpMonitor : IStreamTestMonitor
    {
        private readonly HttpClient httpClient;
        private readonly string testInformationEndpointUrl;
        private readonly string streamName;
        private int _currentCheckpoint;

        public StreamTestHttpMonitor(
            HttpClient httpClient,
            string streamName,
            string testInformationEndpointUrl = "/testInformation"
            )
        {
            this.httpClient = httpClient;
            this.testInformationEndpointUrl = testInformationEndpointUrl;
            this.streamName = streamName;
        }

        public async Task WaitForCheckpoint(CancellationToken cancellationToken = default)
        {
            do
            {
                cancellationToken.ThrowIfCancellationRequested();
                var result = await httpClient.GetFromJsonAsync<StreamTestInformation>($"{testInformationEndpointUrl}?stream={streamName}");

                if (result == null)
                {
                    throw new Exception("Failed to get test information");
                }
                if (result.LatestException != null)
                {
                    throw new Exception(result.LatestException);
                }
                if (result.CheckpointVersion > _currentCheckpoint)
                {
                    _currentCheckpoint = (int)result.CheckpointVersion;
                    return;
                }
            } while (true);
        }

    }
}
