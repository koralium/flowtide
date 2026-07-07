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

using FlowtideDotNet.Core.Sources.Generic;
using System.Threading.Channels;

namespace CustomSinkSample
{
    public class ConsoleInputSource : GenericDataSourceAsync<InputModel>
    {
        private Channel<InputModel> _channel = Channel.CreateUnbounded<InputModel>();
        private readonly List<InputModel> _initialData;

        public ConsoleInputSource(List<InputModel> initialData)
        {
            this._initialData = initialData;
        }

        public ValueTask EnqueueData(InputModel data)
        {
            return _channel.Writer.WriteAsync(data);
        }

        public override bool IsPushBased => true;

        public override TimeSpan? DeltaLoadInterval => TimeSpan.FromSeconds(1);

        public override async Task RunDeltaLoad(IDeltaLoadContext<InputModel> context, CancellationToken cancellationToken)
        {
            // Wait for input from the channel
            while(await _channel.Reader.WaitToReadAsync(cancellationToken))
            {
                // Begin transaction to take lock so all data is sent inside one checkpoint
                await using var transaction = await context.BeginTransactionAsync(cancellationToken);

                while (_channel.Reader.TryRead(out var item))
                {
                    if (item.Crash)
                    { 
                        throw new Exception("User crash requested");
                    }
                    if (item.Id == null)
                    {
                        throw new InvalidOperationException("InputModel Id cannot be null");
                    }
                    var flowtideObject = new FlowtideGenericObject<InputModel>(item.Id, item, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), item.IsDeleted);
                    await transaction.SubmitAsync(flowtideObject, cancellationToken);
                }
            }
        }

        public override IAsyncEnumerable<FlowtideGenericObject<InputModel>> FullLoadAsync()
        {
            return _initialData.ToAsyncEnumerable().Select(item =>
            {
                if (item.Id == null)
                {
                    throw new InvalidOperationException("InputModel Id cannot be null");
                }
                return new FlowtideGenericObject<InputModel>(item.Id, item, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), item.IsDeleted);
            });
        }
    }
}
