using FlowtideDotNet.Base.Engine;

namespace FlowtideDotNet.TestFramework
{
    public class FlowtideTestStream
    {
        private readonly Base.Engine.DataflowStream dataflowStream;
        private long _lastSeenCheckpointVersion = 0;

        public FlowtideTestStream(Base.Engine.DataflowStream dataflowStream)
        {
            this.dataflowStream = dataflowStream;
        }

        public async Task WaitForCheckpoint()
        {
            _lastSeenCheckpointVersion = await dataflowStream.WaitForCheckpointTime(_lastSeenCheckpointVersion + 1);
        }
    }
}
