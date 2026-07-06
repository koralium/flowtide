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

using FlowtideDotNet.Base;
using FlowtideDotNet.Core.Sources.Generic;
using Microsoft.Extensions.Logging;

namespace CustomSinkSample
{
    public class CustomSink : GenericDataSink<SinkModel>
    {
        private readonly List<SinkModel> _existingData;
        private readonly ILogger<CustomSink> _logger;

        public CustomSink(List<SinkModel> existingData, ILogger<CustomSink> logger)
        {
            this._existingData = existingData;
            this._logger = logger;
        }

        public override IAsyncEnumerable<SinkModel> GetExistingData()
        {
            _logger.LogInformation("Fetching existing data from CustomSink");
            return _existingData.ToAsyncEnumerable();
        }

        public override Task<List<string>> GetPrimaryKeyNames()
        {
            return Task.FromResult(new List<string> { nameof(SinkModel.Id) });
        }

        public override async Task OnChanges(IAsyncEnumerable<FlowtideGenericWriteObject<SinkModel>> changes, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            await foreach (var change in changes)
            {
                if (change.IsDeleted)
                {
                    _logger.LogInformation($"Sink delete received for id '{change.Value.Id}'");
                }
                else
                {
                    _logger.LogInformation($"Sink update received for id '{change.Value.Id}' with name '{change.Value.Name}'");
                }
            }
        }
    }
}
