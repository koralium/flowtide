using Apache.Arrow.Ipc;
using BenchmarkDotNet.Attributes;
using FASTER.core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Buffers;
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
        private ArrayBufferWriter<byte> _bufferWriter = new ArrayBufferWriter<byte>();
        private byte[]? _toDeserialize;

        [GlobalSetup]
        public void GlobalSetup()
        {
            Column[] columns = new Column[3];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
            }

            for (int i = 0; i < 8000; i++)
            {
                columns[0].Add(new Int64Value(i));
                columns[1].Add(new Int64Value(i));
                columns[2].Add(new StringValue("abcdef"));
            }

            _eventBatchData = new EventBatchData(columns);
            _recordBatch = EventArrowSerializer.BatchToArrow(_eventBatchData, _eventBatchData.Count);

            _bufferWriter.ResetWrittenCount();
            eventBatchSerializer.SerializeEventBatch(_bufferWriter, _eventBatchData, _eventBatchData.Count);
            _toDeserialize = _bufferWriter.WrittenSpan.ToArray();
            _bufferWriter.ResetWrittenCount();
        }

        //[Benchmark]
        //public void FlowtideSerializer()
        //{
        //    Debug.Assert(_eventBatchData != null);
        //    _bufferWriter.ResetWrittenCount();
        //    eventBatchSerializer.SerializeEventBatch(_bufferWriter, _eventBatchData, _eventBatchData.Count);
        //}

        [Benchmark]
        public void FlowtideDeserialize()
        {
            Debug.Assert(_toDeserialize != null);
            var deserializer = new EventBatchDeserializer(GlobalMemoryManager.Instance, new SequenceReader<byte>(new ReadOnlySequence<byte>(_toDeserialize)));
            var batch = deserializer.DeserializeBatch();
            batch.Dispose();
        }

        //[Benchmark]
        //public void ArrowSerializer()
        //{
        //    Debug.Assert(_recordBatch != null);
        //    _memoryStream.SetLength(0);
        //    var batchWriter = new ArrowStreamWriter(_memoryStream, _recordBatch.Schema, true);
        //    batchWriter.WriteRecordBatch(_recordBatch);
        //    _memoryStream.ToArray();
        //}

        [Benchmark]
        public void ArrowDeserialize()
        {
            Debug.Assert(_toDeserialize != null);
            var stream = new MemoryStream(_toDeserialize);
            var reader = new ArrowStreamReader(stream);
            var batch = reader.ReadNextRecordBatch();
            EventArrowSerializer.ArrowToBatch(batch, GlobalMemoryManager.Instance);
            batch.Dispose();
        }

        //[Benchmark]
        //public void ConvertToArrowSerialize()
        //{
        //    Debug.Assert(_eventBatchData != null);
        //    _memoryStream.SetLength(0);
        //    var recordBatch = EventArrowSerializer.BatchToArrow(_eventBatchData, _eventBatchData.Count);
        //    var batchWriter = new ArrowStreamWriter(_memoryStream, recordBatch.Schema, true);
        //    batchWriter.WriteRecordBatch(recordBatch);
        //    _memoryStream.ToArray();
        //}
    }
}
