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

using FlowtideDotNet.Connector.StarRocks.Internal.HttpApi;
using Microsoft.Extensions.Logging;
using Polly;

namespace FlowtideDotNet.Connector.StarRocks.Internal
{
    /// <summary>
    /// Runs every request through the pipeline, logs each failed attempt.
    /// </summary>
    internal sealed class ResilientStarRocksClient : IStarrocksClient
    {
        private readonly IStarrocksClient _inner;
        private readonly ResiliencePipeline _pipeline;
        private readonly ILogger _logger;
        private readonly string _streamName;
        private readonly string _operatorId;
        private readonly CancellationToken _cancellationToken;

        public ResilientStarRocksClient(
            IStarrocksClient inner,
            ResiliencePipeline pipeline,
            ILogger logger,
            string streamName,
            string operatorId,
            CancellationToken cancellationToken)
        {
            _inner = inner;
            _pipeline = pipeline;
            _logger = logger;
            _streamName = streamName;
            _operatorId = operatorId;
            _cancellationToken = cancellationToken;
        }

        public Task<StreamLoadInfo> TransactionCommit(StarRocksTransactionId transactionId)
        {
            return Execute(nameof(TransactionCommit), () => _inner.TransactionCommit(transactionId));
        }

        public Task<TransactionInfo> CreateTransaction(StarRocksTransactionId transactionId)
        {
            return Execute(nameof(CreateTransaction), () => _inner.CreateTransaction(transactionId));
        }

        public Task TransactionLoad(StarRocksTransactionLoadInfo request)
        {
            return Execute(nameof(TransactionLoad), () => _inner.TransactionLoad(request));
        }

        public Task<TransactionInfo> TransactionPrepare(StarRocksTransactionId transactionId)
        {
            return Execute(nameof(TransactionPrepare), () => _inner.TransactionPrepare(transactionId));
        }

        public Task<TransactionInfo> TransactionRollback(StarRocksTransactionId transactionId)
        {
            return Execute(nameof(TransactionRollback), () => _inner.TransactionRollback(transactionId));
        }

        public Task StreamLoad(StarRocksStreamLoadInfo request)
        {
            return Execute(nameof(StreamLoad), () => _inner.StreamLoad(request));
        }

        public Task<TableInfo> GetTableInfo(List<string> names)
        {
            return Execute(nameof(GetTableInfo), () => _inner.GetTableInfo(names));
        }

        // A new request per attempt, a started task cannot be retried
        private async Task<T> Execute<T>(string operation, Func<Task<T>> request)
        {
            int attempt = 0;
            return await _pipeline.ExecuteAsync(async token =>
            {
                attempt++;
                try
                {
                    return await request();
                }
                catch (Exception e) when (!token.IsCancellationRequested)
                {
                    _logger.StarRocksRequestFailed(e, operation, attempt, _streamName, _operatorId);
                    throw;
                }
            }, _cancellationToken);
        }

        private async Task Execute(string operation, Func<Task> request)
        {
            int attempt = 0;
            await _pipeline.ExecuteAsync(async token =>
            {
                attempt++;
                try
                {
                    await request();
                }
                catch (Exception e) when (!token.IsCancellationRequested)
                {
                    _logger.StarRocksRequestFailed(e, operation, attempt, _streamName, _operatorId);
                    throw;
                }
            }, _cancellationToken);
        }
    }
}
