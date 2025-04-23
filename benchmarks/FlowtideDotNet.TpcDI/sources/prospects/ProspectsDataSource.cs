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

using CsvHelper.Configuration;
using CsvHelper;
using FlowtideDotNet.Core.Sources.Generic;
using Stowage;
using System.Globalization;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Storage.StateManager;
using System.Diagnostics;

namespace FlowtideDotNet.TpcDI.sources.prospects
{
    /// <summary>
    /// Prospects use a custom data source to handle bulk loading and handle date updates
    /// </summary>
    public class ProspectsDataSource : GenericDataSourceAsync<Prospect>
    {
        private readonly IFileStorage fileStorage;
        private IObjectState<long>? _batchId;

        public ProspectsDataSource(IFileStorage fileStorage)
        {
            this.fileStorage = fileStorage;
        }
        public override TimeSpan? DeltaLoadInterval => default;

        private async Task<DateTimeOffset> GetBatchDate(long batchId)
        {
            using var stream = await fileStorage.OpenRead($"Batch{batchId}/BatchDate.txt");
            if (stream == null)
            {
                throw new InvalidOperationException($"BatchDate.txt not found in Batch{batchId}");
            }
            // Read the line
            using var reader = new StreamReader(stream);
            var line = await reader.ReadLineAsync();
            if (line == null)
            {
                throw new InvalidOperationException($"BatchDate.txt is empty in Batch{batchId}");
            }

            // Parse the date
            if (!DateTimeOffset.TryParse(line, out var date))
            {
                throw new InvalidOperationException($"BatchDate.txt is not a valid date in Batch{batchId}");
            }

            return date;
        }

        public override async Task Checkpoint()
        {
            Debug.Assert(_batchId != null, "BatchId is not initialized");
            await _batchId.Commit();
        }

        public override async IAsyncEnumerable<FlowtideGenericObject<Prospect>> DeltaLoadAsync(long lastWatermark)
        {
            Debug.Assert(_batchId != null, "BatchId is not initialized");

            var nextBatchId = _batchId.Value + 1;

            var stream = await fileStorage.OpenRead($"Batch{nextBatchId}/Prospect.csv");

            if (stream == null)
            {
                yield break;
            }
            _batchId.Value = nextBatchId;
            var batchDate = await GetBatchDate(nextBatchId);
            var config = new CsvConfiguration(CultureInfo.InvariantCulture) { Delimiter = ",", HasHeaderRecord = false };

            using (var reader = new StreamReader(stream))
            using (var csv = new CsvReader(reader, config))
            {
                while (await csv.ReadAsync())
                {
                    var prospect = csv.GetRecord<Prospect>();
                    var existingProspect = await LookupRow(prospect.AgencyID!);

                    if (existingProspect == null)
                    {
                        prospect.RecordDate = batchDate;
                        prospect.UpdateDate = batchDate;
                        prospect.BatchID = (int)_batchId.Value;
                    }
                    else
                    {
                        if (prospect.Equals(existingProspect))
                        {
                            prospect.RecordDate = batchDate;
                        }
                        else
                        {
                            prospect.RecordDate = batchDate;
                            prospect.UpdateDate = batchDate;
                            prospect.BatchID = (int)_batchId.Value;
                        }
                    }
                    
                    yield return new FlowtideGenericObject<Prospect>(prospect.AgencyID!, prospect, _batchId.Value, false);
                }
            }
        }

        public override async IAsyncEnumerable<FlowtideGenericObject<Prospect>> FullLoadAsync()
        {
            Debug.Assert(_batchId != null, "BatchId is not initialized");

            if (_batchId.Value > 0)
            {
                yield break;
            }

            var batchDate = await GetBatchDate(1);

            var stream = await fileStorage.OpenRead($"Batch1/Prospect.csv");

            if (stream == null)
            {
                throw new FileNotFoundException($"Prospect file not found.");
            }

            var config = new CsvConfiguration(CultureInfo.InvariantCulture) { Delimiter = ",", HasHeaderRecord = false };

            _batchId.Value = 1;
            using (var reader = new StreamReader(stream))
            using (var csv = new CsvReader(reader, config))
            {
                while (await csv.ReadAsync())
                {
                    var prospect = csv.GetRecord<Prospect>();
                    prospect.RecordDate = batchDate;
                    prospect.UpdateDate = batchDate;
                    prospect.BatchID = 1;
                    yield return new FlowtideGenericObject<Prospect>(prospect.AgencyID!, prospect, 1, false);
                }
            }
        }

        public override async Task Initialize(ReadRelation readRelation, IStateManagerClient stateManagerClient)
        {
            _batchId = await stateManagerClient.GetOrCreateObjectStateAsync<long>("batch_id");
        }
    }
}
