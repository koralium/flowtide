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

using FlowtideDotNet.DependencyInjection;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Storage;
using Orleans.TestingHost;
using System.Collections.Concurrent;

namespace FlowtideDotNet.Cluster.Orleans.Tests
{
    public abstract class OrleansClusterFixtureBase : IDisposable
    {
        public TestCluster Cluster { get; }

        protected OrleansClusterFixtureBase(short siloCount, bool durableStreamState = false)
        {
            // Real delays, only shorter: every recovery hop pays the restart settle delay
            // and every stream start pays a handshake retry slice for the substream that
            // loses the startup race. The test cluster silos run in process, the statics
            // reach the grains.
            FlowtideDotNet.Base.Engine.Internal.StateMachine.FailureStreamState.RecoveryRestartDelay = TimeSpan.FromMilliseconds(50);
            FlowtideDotNet.Core.Operators.Exchange.SubstreamCommunicationPoint.NotStartedRetrySliceMs = 50;

            var builder = new TestClusterBuilder(siloCount);
            if (durableStreamState)
            {
                builder.AddSiloBuilderConfigurator<DurableStorageSiloConfigurator>();
            }
            else
            {
                builder.AddSiloBuilderConfigurator<TestSiloConfigurator>();
            }
            Cluster = builder.Build();
            Cluster.Deploy();
        }

        public void Dispose()
        {
            Cluster.StopAllSilos();
            Cluster.Dispose();
        }

        private sealed class TestSiloConfigurator : ISiloConfigurator
        {
            public void Configure(ISiloBuilder siloBuilder)
            {
                ConfigureSilo(siloBuilder, (streamName, substreamName, storage) =>
                {
                    storage.AddTemporaryDevelopmentStorage(options =>
                    {
                        // Unique per stream instance: the temporary development storage
                        // deletes its files on dispose, so state never survives a restart
                        // anyway, and after a silo failure a new activation can start while
                        // the old activations files are not released yet, a shared directory
                        // would collide on the files.
                        options.DirectoryPath = $"./temp/orleans_tests/{streamName}/{substreamName}/{Guid.NewGuid():N}";
                    });
                    // A substream stopping alone, for example when its silo shuts down, waits
                    // for peer stop barriers that never come since the peers keep running, a
                    // short drain timeout keeps deactivation fast in tests.
                }, stopDrainTimeout: TimeSpan.FromSeconds(2));
            }
        }

        /// <summary>
        /// Stream state storage that survives grain deactivation, like a production deployment.
        /// In memory but shared across the cluster's silos, so an activation moved to another
        /// silo restores its predecessor's state, which a planned migration handoff relies on.
        /// </summary>
        private sealed class DurableStorageSiloConfigurator : ISiloConfigurator
        {
            private static readonly ConcurrentDictionary<string, KeepAliveMemoryFileProvider> _providers = new();

            public void Configure(ISiloBuilder siloBuilder)
            {
                ConfigureSilo(siloBuilder, (streamName, substreamName, storage) =>
                {
                    var provider = _providers.GetOrAdd($"{streamName}/{substreamName}", _ => new KeepAliveMemoryFileProvider());
                    storage.SetPersistentStorage(new ReservoirPersistentStorage(new ReservoirStorageOptions
                    {
                        FileProvider = provider
                    }));
                    storage.ZstdPageCompression();
                    // The production default: tests here stop coordinated so the drain
                    // completes on its own. A short timeout would fail a merely-slow stop under
                    // parallel load and turn a clean handoff into a rollback.
                }, stopDrainTimeout: TimeSpan.FromSeconds(30));
            }
        }

        /// <summary>
        /// A memory file provider whose contents survive the owning storage being disposed, so
        /// a migrated activation restores what its predecessor persisted, like durable storage.
        /// Relisting the interface remaps its Dispose (called only through it) to the no-op here.
        /// </summary>
        private sealed class KeepAliveMemoryFileProvider : MemoryFileProvider, IReservoirStorageProvider
        {
            public new void Dispose()
            {
            }
        }

        private static void ConfigureSilo(ISiloBuilder siloBuilder, Action<string, string, IFlowtideStorageBuilder> streamStateStorage, TimeSpan stopDrainTimeout)
        {
            siloBuilder.Services.AddLogging(logging =>
            {
                logging.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Debug);
                logging.AddFilter("Orleans", Microsoft.Extensions.Logging.LogLevel.Warning);
                logging.AddFilter("Microsoft", Microsoft.Extensions.Logging.LogLevel.Warning);
                // In memory ring buffer, dumped by a test when it fails. A file logger
                // slows the streams down enough to hide timing sensitive failures.
                logging.AddProvider(new SharedRingBufferLogger());
                if (Environment.GetEnvironmentVariable("FLOWTIDE_ORLEANS_TEST_LOG") is string logPath && logPath.Length > 0)
                {
                    logging.AddProvider(new SharedFileLoggerProvider(logPath));
                }
            });
            // Grain state must survive a silo stopping so grains can reactivate on the
            // remaining silos, the Orleans memory grain storage loses its partitions with
            // the silo that hosted them. The same applies to reminders, which back the
            // substream keep alive that reactivates grains from a lost silo.
            siloBuilder.Services.AddKeyedSingleton<IGrainStorage>("stream_metadata", (_, _) => new SharedInMemoryGrainStorage());
            siloBuilder.AddReminders();
            siloBuilder.Services.AddSingleton<IReminderTable, SharedInMemoryReminderTable>();
            // The reminder service only re-reads the table on the refresh period, with
            // the default 5 minutes a surviving silo takes over the reminders of a lost
            // silo far too late for the tests.
            siloBuilder.Configure<ReminderOptions>(options =>
            {
                options.RefreshReminderListPeriod = TimeSpan.FromSeconds(15);
            });
            // Calls to grains that were hosted on a lost silo fail with the response
            // timeout, the default 30 seconds makes recovering from a silo failure very
            // slow since several such calls fail in sequence before the stream recovers.
            siloBuilder.Configure<SiloMessagingOptions>(options =>
            {
                options.ResponseTimeout = TimeSpan.FromSeconds(10);
            });
            // The stream name aware overload so the per stream connector routing is
            // exercised by every test.
            siloBuilder.Services.AddFlowtideOrleans((streamName, connectors) =>
            {
                connectors.AddSource(new TestDataSourceFactory("*"));
                connectors.AddSink(new TestDataSinkFactory("*"));
            }, streamStateStorage, options =>
            {
                options.ConfigureBuilder = (streamName, substreamName, builder) => builder.SetStopDrainTimeout(stopDrainTimeout);
            });
        }
    }

    /// <summary>
    /// Test cluster with a single silo, grain messages between grains on the same silo pass
    /// by reference.
    /// </summary>
    public sealed class OrleansClusterFixture : OrleansClusterFixtureBase
    {
        public OrleansClusterFixture() : base(1)
        {
        }
    }

    /// <summary>
    /// Test cluster with two silos, grains are placed across the silos and messages between
    /// them are serialized over the network, which exercises the substream event wire format
    /// the same way a multi node deployment does.
    /// </summary>
    public sealed class OrleansTwoSiloClusterFixture : OrleansClusterFixtureBase
    {
        public OrleansTwoSiloClusterFixture() : base(2)
        {
        }
    }

    /// <summary>
    /// Two silo cluster whose stream state storage survives grain deactivation. Used by tests
    /// that move activations between silos and rely on the new activation restoring the state,
    /// which the throwaway storage of the other fixtures never provides.
    /// </summary>
    public sealed class OrleansTwoSiloDurableStorageClusterFixture : OrleansClusterFixtureBase
    {
        public OrleansTwoSiloDurableStorageClusterFixture() : base(2, durableStreamState: true)
        {
        }
    }
}
