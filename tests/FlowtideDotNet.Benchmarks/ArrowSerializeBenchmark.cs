using Apache.Arrow.Ipc;
using BenchmarkDotNet.Attributes;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Benchmarks
{
    public class ArrowSerializeBenchmark
    {
        EventBatchData? _eventBatchData;
        EventBatchSerializer eventBatchSerializer = new EventBatchSerializer();
        Apache.Arrow.RecordBatch? _recordBatch;
        MemoryStream _memoryStream = new MemoryStream();

        [GlobalSetup]
        public void GlobalSetup()
        {
            Column[] columns = new Column[2];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
            }

            for (int i = 0; i < 8000; i++)
            {
                columns[0].Add(new Int64Value(i));
                columns[1].Add(new Int64Value(i));
            }

            _eventBatchData = new EventBatchData(columns);
            _recordBatch = EventArrowSerializer.BatchToArrow(_eventBatchData, _eventBatchData.Count);
        }

        [Benchmark]
        public int FlowtideSerializer()
        {
            Debug.Assert(_eventBatchData != null);
            var serializationEstimation = eventBatchSerializer.GetSerializationEstimation(_eventBatchData);
            eventBatchSerializer.SerializeSchema(_eventBatchData, serializationEstimation);
            var buffer = eventBatchSerializer.SerializeRecordBatch(_eventBatchData, _eventBatchData.Count, serializationEstimation);
            return buffer.Length;
        }

        [Benchmark]
        public void ArrowSerializer()
        {
            Debug.Assert(_recordBatch != null);
            _memoryStream.SetLength(0);
            var batchWriter = new ArrowStreamWriter(_memoryStream, _recordBatch.Schema, true);
            batchWriter.WriteRecordBatch(_recordBatch);
        }

        [Benchmark]
        public void ConvertToArrowSerialize()
        {
            Debug.Assert(_eventBatchData != null);
            _memoryStream.SetLength(0);
            var recordBatch = EventArrowSerializer.BatchToArrow(_eventBatchData, _eventBatchData.Count);
            var batchWriter = new ArrowStreamWriter(_memoryStream, recordBatch.Schema, true);
            batchWriter.WriteRecordBatch(recordBatch);
        }
    }
}
