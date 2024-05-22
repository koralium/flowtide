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

using FlexBuffers;
using FlowtideDotNet.Base;
using FlowtideDotNet.Storage.Tree;
using SqlParser.Ast;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class StreamEventSerializer : IBplusTreeSerializer<IStreamEvent>
    {
        private const byte StreamEventBatchType = 0;
        private const byte WatermarkType = 1;
        private const byte LockingEventPrepareType = 2;
        private const byte InitWatermarksEventType = 3;
        private const byte CheckpointType = 4;

        public void Deserialize(in BinaryReader reader, in List<IStreamEvent> values)
        {
            var count = reader.ReadInt32();

            for (var i = 0; i < count; i++)
            {
                var type = reader.ReadByte();
                //switch (type)
                //{
                //    case StreamEventBatchType:
                //        DeserializeBatch(reader, values);
                //        break;
                //    case WatermarkType:
                //        DeserializeWatermark(reader, values);
                //        break;
                //    case LockingEventPrepareType:
                //        DeserializeLockingEventPrepare(reader, values);
                //        break;
                //    case InitWatermarksEventType:
                //        DeserializeInitWatermarksEvent(reader, values);
                //        break;
                //    case CheckpointType:
                //        DeserializeCheckpoint(reader, values);
                //        break;
                //    default:
                //        throw new NotImplementedException();
                //}
            }

            throw new NotImplementedException();
        }

        //private void DeserializeBatch(in BinaryReader reader, in List<IStreamEvent> values)
        //{
        //    var time = reader.ReadInt64();
        //    var length = reader.ReadInt32();
        //    var bytes = reader.ReadBytes(length);
        //    var vector = FlxValue.FromMemory(bytes).AsVector;

        //    var events = new List<RowEvent>();
        //    for (var i = 0; i < vector.Length; i++)
        //    {
        //        events.Add(new RowEvent(new VectorRowData(vector.Get(i).AsVector)));
        //    }

        //    values.Add(new StreamMessage<StreamEventBatch>(new StreamEventBatch(events), time));
        //}

        private void SerializeBatch(in BinaryWriter writer, in StreamMessage<StreamEventBatch> batch)
        {
            writer.Write(StreamEventBatchType);
            writer.Write(batch.Time);

            var builder = new FlexBuffer(ArrayPool<byte>.Shared, options: FlexBuffer.Options.ShareKeys | FlexBuffer.Options.ShareStrings | FlexBuffer.Options.ShareKeyVectors);
            builder.NewObject();
            var startRoot = builder.StartVector();
            foreach (var value in batch.Data.Events)
            {
                var elementStart = builder.StartVector();
                for (int i = 0; i < value.Length; i++)
                {
                    builder.Add(value.GetColumn(i));
                }
                builder.EndVector(elementStart, false, false);
            }
            builder.EndVector(startRoot, false, false);
            var bytes = builder.Finish();
            writer.Write(bytes.Length);
            writer.Write(bytes);
        }

        private void SerializeWatermark(in BinaryWriter writer, Watermark watermark)
        {
            writer.Write(WatermarkType);
            writer.Write(watermark.SourceOperatorId ?? "");
            writer.Write(watermark.StartTime.ToUnixTimeMilliseconds());

            writer.Write(watermark.Watermarks.Count);

            foreach (var wm in watermark.Watermarks)
            {
                writer.Write(wm.Key);
                writer.Write(wm.Value);
            }
        }

        private void SerializeCheckpoint(in BinaryWriter writer, Checkpoint checkpoint)
        {
            writer.Write(CheckpointType);
            writer.Write(checkpoint.CheckpointTime);
            writer.Write(checkpoint.NewTime);
        }

        private void SerializeLockingEvent(in BinaryWriter writer, ILockingEvent lockingEvent)
        {
            if (lockingEvent is InitWatermarksEvent initWatermarksEvent)
            {
                SerializeInitWatermarksEvent(writer, initWatermarksEvent);
                return;
            }
            if (lockingEvent is Checkpoint checkpointEvent)
            {
                SerializeCheckpoint(writer, checkpointEvent);
                return;
            }
            

            throw new NotImplementedException();
        }

        private void SerializeInitWatermarksEvent(in BinaryWriter writer, InitWatermarksEvent initWatermarksEvent)
        {
            writer.Write(InitWatermarksEventType);
            writer.Write(initWatermarksEvent.WatermarkNames.Count);

            foreach (var wm in initWatermarksEvent.WatermarkNames)
            {
                writer.Write(wm);
            }
        }

        private void SerializeLockingEventPrepare(in BinaryWriter writer, LockingEventPrepare lockingEventPrepare)
        {
            writer.Write(LockingEventPrepareType);
            writer.Write(lockingEventPrepare.OtherInputsNotInCheckpoint);
            SerializeLockingEvent(writer, lockingEventPrepare.LockingEvent);
        }

        public void Serialize(in BinaryWriter writer, in List<IStreamEvent> values)
        {
            writer.Write(values.Count);
            foreach (var value in values)
            {
                if (value is StreamMessage<StreamEventBatch> batch)
                {
                    SerializeBatch(writer, batch);
                }
                else if (value is Watermark watermark)
                {
                    SerializeWatermark(writer, watermark);
                }
                else if (value is LockingEventPrepare lockingEventPrepare)
                {
                    SerializeLockingEventPrepare(writer, lockingEventPrepare);
                }
                else if (value is ILockingEvent lockingEvent)
                {
                    SerializeLockingEvent(writer, lockingEvent);
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
            throw new NotImplementedException();
        }
    }
}
