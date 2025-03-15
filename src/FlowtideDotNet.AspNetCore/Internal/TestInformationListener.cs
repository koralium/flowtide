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

using FlowtideDotNet.AspNetCore.Testing;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.AspNetCore.Internal
{
    internal class TestInformationListener
    {
        private readonly MeterListener _meterListener;
        private readonly ActivityListener _activityListener;

        private ConcurrentDictionary<string, long> _checkpointVersions;
        private ConcurrentDictionary<string, Exception> _latestFailure;

        public TestInformationListener()
        {
            _checkpointVersions = new ConcurrentDictionary<string, long>();
            _latestFailure = new ConcurrentDictionary<string, Exception>();
            _meterListener = new MeterListener();

            _meterListener.InstrumentPublished = OnInstrumentPublished;

            _activityListener = new ActivityListener()
            {
                ShouldListenTo = activitySource =>
                {
                    if (activitySource.Name == "FlowtideDotNet.Base.StreamException")
                    {
                        return true;
                    }
                    return false;
                },
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                ActivityStopped = activity =>
                {
                    if (activity.OperationName == "StreamFailure")
                    {
                        var streamName = activity.GetTagItem("stream")?.ToString();
                        if (streamName == null)
                        {
                            return;
                        }
                        var exception = activity.GetTagItem("exception") as Exception;
                        if (exception == null)
                        {
                            return;
                        }
                        _latestFailure.AddOrUpdate(streamName, exception, (key, value) => exception);
                    }
                }
            };
            
            ActivitySource.AddActivityListener(_activityListener);

            _meterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
            {
                if (instrument.Name == "flowtide_stream_checkpoint_version")
                {
                    for (int i = 0; i < tags.Length; i++)
                    {
                        if (tags[i].Key == "stream")
                        {
                            var streamName = tags[i].Value?.ToString();
                            if (streamName != null)
                            {
                                _checkpointVersions.AddOrUpdate(streamName, measurement, (key, value) => measurement);
                            }
                            break;
                        }
                    }
                }
            });

            _meterListener.Start();
        }

        private void OnInstrumentPublished(Instrument instrument, MeterListener meterListener)
        {
            if (instrument.Name == "flowtide_stream_checkpoint_version")
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        }

        public bool TryGetCheckpointVersion(string streamName, [NotNullWhen(true)] out StreamTestInformation? information)
        {
            _meterListener.RecordObservableInstruments();
            bool hasCheckpointVersion = _checkpointVersions.TryGetValue(streamName, out var checkpointVersion);

            if (!hasCheckpointVersion)
            {
                information = default;
                return false;
            }

            _latestFailure.TryGetValue(streamName, out var latestFailure);

            information = new StreamTestInformation(checkpointVersion, latestFailure?.ToString());
            return true;
        }
    }
}
