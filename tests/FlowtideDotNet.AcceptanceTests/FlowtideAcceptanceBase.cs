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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.AcceptanceTests.Entities.tpcdi;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Substrait.Sql;
using System.Diagnostics;
using System.Reflection;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    public class FlowtideAcceptanceBase : IAsyncLifetime
    {
        private readonly FlowtideTestStream flowtideTestStream;

        public IReadOnlyList<User> Users => flowtideTestStream.Users;
        public IReadOnlyList<Order> Orders => flowtideTestStream.Orders;
        public IReadOnlyList<Company> Companies => flowtideTestStream.Companies;
        public IReadOnlyList<Project> Projects => flowtideTestStream.Projects;
        public IReadOnlyList<ProjectMember> ProjectMembers => flowtideTestStream.ProjectMembers;

        public IReadOnlyList<GraphNode> GraphNodes => flowtideTestStream.GraphNodes;

        public IReadOnlyList<Security> Securities => flowtideTestStream.Securities;

        public IReadOnlyList<DailyMarket> DailyMarkets => flowtideTestStream.DailyMarkets;

        public IFunctionsRegister FunctionsRegister => flowtideTestStream.FunctionsRegister;
        public ISqlFunctionRegister SqlFunctionRegister => flowtideTestStream.SqlFunctionRegister;

        public Watermark? LastWatermark => flowtideTestStream.LastWatermark;

        public StreamStateValue State => flowtideTestStream.State;

        protected Task StartStream(
            string sql,
            int parallelism = 1,
            StateSerializeOptions? stateSerializeOptions = default,
            int pageSize = 1024,
            bool ignoreSameDataCheck = false,
            ICheckFailureListener? failureListener = default,
            PlanOptimizerSettings? planOptimizerSettings = default,
            DistributedOptions? distributedOptions = default) => flowtideTestStream.StartStream(sql, parallelism, stateSerializeOptions, default, pageSize, ignoreSameDataCheck, failureListener, planOptimizerSettings, distributedOptions: distributedOptions);


        protected Task StopStream() => flowtideTestStream.StopStream();

        protected Task InjectFailure(Exception exception) => flowtideTestStream.InjectFailure(exception);

        protected void InjectEgressCheckpointDone(string operatorName, FlowtideDotNet.Base.ILockingEvent? lockingEvent) => flowtideTestStream.InjectEgressCheckpointDone(operatorName, lockingEvent);

        protected void Pause() => flowtideTestStream.Pause();

        protected void Resume() => flowtideTestStream.Resume();

        /// <summary>
        /// Allows the stream to fail and recover without failing the test.
        /// Used by tests that expect a fail and recover, such as distributed version mismatch tests.
        /// </summary>
        protected void AllowFailureAndRecover() => flowtideTestStream.AllowFailureAndRecover = true;

        protected Task StartStream() => flowtideTestStream.StartStream();

        /// <summary>
        /// Makes the sinks DeleteAsync throw this many times, set before StartStream.
        /// </summary>
        protected int SinkDeleteFailCount { set => flowtideTestStream.SinkDeleteFailCount = value; }

        /// <summary>
        /// Enables the checkpoint-after-initial-data stream option, set before StartStream.
        /// </summary>
        protected bool WaitForCheckpointAfterInitialData { set => flowtideTestStream.WaitForCheckpointAfterInitialData = value; }

        /// <summary>
        /// Delays every source's initial data send, keeping the stream in startup, set before
        /// StartStream.
        /// </summary>
        protected TimeSpan? InitialDataDelay { set => flowtideTestStream.InitialDataDelay = value; }

        /// <summary>
        /// Overrides the mock source's batch flush size, set before StartStream.
        /// </summary>
        protected int? SourceBatchSize { set => flowtideTestStream.SourceBatchSize = value; }

        /// <summary>
        /// Sets the minimum time between checkpoint triggers, set before StartStream.
        /// </summary>
        protected TimeSpan? MinimumTimeBetweenCheckpoints { set => flowtideTestStream.MinimumTimeBetweenCheckpoints = value; }

        /// <summary>
        /// Sets the stop drain timeout, which also bounds a stop deferred behind an
        /// in-progress checkpoint. Set before StartStream.
        /// </summary>
        protected TimeSpan? StopDrainTimeout { set => flowtideTestStream.StopDrainTimeout = value; }

        public EventBatchData GetActualRows() => flowtideTestStream.GetActualRowsAsVectors();

        protected void AssertCurrentDataEqual<T>(IEnumerable<T> data)
        {
            flowtideTestStream.AssertCurrentDataEqual(data);
        }

        public void EnterDataWriteLock()
        {
            flowtideTestStream.EnterDataWriteLock();
        }

        public void ExitDataWriteLock()
        {
            flowtideTestStream.ExitDataWriteLock();
        }

        protected void GenerateData(int count = 1000)
        {
            flowtideTestStream.Generate(count);
        }

        protected void SetPageSizeBytes(int bytes)
        {
            flowtideTestStream.BPlusTreePageSizeBytes = bytes;
        }

        protected void GenerateUsers(int count = 1000)
        {
            flowtideTestStream.GenerateUsers(count);
        }

        protected void GenerateOrders(int count = 1000)
        {
            flowtideTestStream.GenerateOrders(count);
        }

        protected void GenerateCompanies(int count = 1)
        {
            flowtideTestStream.GenerateCompanies(count);
        }

        protected void GenerateProjects(int count = 1000)
        {
            flowtideTestStream.GenerateProjects(count);
        }

        protected void GenerateProjectMembers(int count = 1000)
        {
            flowtideTestStream.GenerateProjectMembers(count);
        }

        protected void GenerateGraphNodes(int count = 1000)
        {
            flowtideTestStream.GenerateGraphNodes(count);
        }

        protected void AddOrUpdateCompany(Company company)
        {
            flowtideTestStream.AddOrUpdateCompany(company);
        }

        protected Task WaitForUpdate()
        {
            return flowtideTestStream.WaitForUpdate();
        }

        public void WaitForUpdateDoesNotRequireDataChange()
        {
            flowtideTestStream.WaitForUpdateDoesNotRequireDataChange();
        }

        protected Task Crash()
        {
            return flowtideTestStream.Crash();
        }

        protected Task SchedulerTick()
        {
            return flowtideTestStream.SchedulerTick();
        }

        protected Task FireCrashTrigger()
        {
            return flowtideTestStream.FireCrashTrigger();
        }

        protected Task StopMockIngressAutocompleteDependencies()
        {
            return flowtideTestStream.StopMockIngressAutocompleteDependencies();
        }

        protected Task MockIngressFailAndRollback(long restoreVersion)
        {
            return flowtideTestStream.MockIngressFailAndRollback(restoreVersion);
        }

        protected Task MockIngressSetDependenciesDone()
        {
            return flowtideTestStream.MockIngressSetDependenciesDone();
        }

        protected void EgressCrashOnCheckpoint(int times)
        {
            flowtideTestStream.EgressCrashOnCheckpoint(times);
        }

        protected Task Trigger(string triggerName, object? state = default)
        {
            return flowtideTestStream.Trigger(triggerName, state);
        }

        public void AddOrUpdateUser(User user)
        {
            flowtideTestStream.AddOrUpdateUser(user);
        }

        public void AddOrUpdateOrder(Order order)
        {
            flowtideTestStream.AddOrUpdateOrder(order);
        }

        public void AddUser(User user)
        {
            flowtideTestStream.AddUser(user);
        }

        public void AddOrder(Order order)
        {
            flowtideTestStream.AddOrder(order);
        }

        public void AddProjects(params Project[] projects)
        {
            flowtideTestStream.AddProjects(projects);
        }

        public void AddProjectMembers(params ProjectMember[] projectMembers)
        {
            flowtideTestStream.AddProjectMembers(projectMembers);
        }

        public void AddOrUpdateProject(Project project)
        {
            flowtideTestStream.AddOrUpdateProject(project);
        }

        public void AddOrUpdateProjectMember(ProjectMember projectMember)
        {
            flowtideTestStream.AddOrUpdateProjectMember(projectMember);
        }

        public void DeleteUser(User user)
        {
            flowtideTestStream.DeleteUser(user);
        }

        public void DeleteOrder(Order order)
        {
            flowtideTestStream.DeleteOrder(order);
        }

        public void AddOrUpdateGraphNode(Entities.GraphNode graphNode)
        {
            flowtideTestStream.AddOrUpdateGraphNode(graphNode);
        }

        public void DeleteGraphNode(Entities.GraphNode graphNode)
        {
            flowtideTestStream.DeleteGraphNode(graphNode);
        }

        public void GenerateTpcDi(int securityCount, int dailyMarketCount)
        {
            flowtideTestStream.GenerateTpcDi(securityCount, dailyMarketCount);
        }

        public void GenerateDailyMarkets(int dailyMarketDays)
        {
            flowtideTestStream.GenerateDailyMarkets(dailyMarketDays);
        }

        public void SourceImmutable()
        {
            flowtideTestStream.SourceImmutable();
        }

        public FlowtideAcceptanceBase(ITestOutputHelper testOutputHelper, bool usePersistentStorage = false)
        {
            FastEngineTimings.Apply();
            var baseType = this.GetType();
            var testName = GetTestClassName(testOutputHelper);
            if (usePersistentStorage)
            {
                flowtideTestStream = new PersistentFlowtideTestStream($"{baseType.Name}/{testName}");
            }
            else
            {
                flowtideTestStream = new FlowtideTestStream($"{baseType.Name}/{testName}");
            }
        }

        private static string GetTestClassName(ITestOutputHelper output)
        {
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            Debug.Assert(testMember != null, "testMember != null");
            var test = (ITest?)testMember.GetValue(output);
            Debug.Assert(test != null, "test != null");
            return test.TestCase.TestMethod.Method.Name;
        }

        public async Task DisposeAsync()
        {

            await flowtideTestStream.DisposeAsync();
        }

        public Task InitializeAsync()
        {
            return Task.CompletedTask;
        }

        public Task DeleteStream()
        {
            return flowtideTestStream.DeleteStream();
        }
    }
}
