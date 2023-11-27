﻿//// Licensed under the Apache License, Version 2.0 (the "License")
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////     http://www.apache.org/licenses/LICENSE-2.0
////  
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.

//using FlowtideDotNet.Storage.Tree;

//namespace FlowtideDotNet.Core.Operators.Set
//{
//    internal class BPlusTreeStreamEventSerializer : IBplusTreeSerializer<RowEvent>
//    {
//        public RowEvent Deserialize(in BinaryReader reader)
//        {
//            var weight = reader.ReadInt32();
//            var iteration = reader.ReadUInt32();
//            var length = reader.ReadInt32();
//            var bytes = reader.ReadBytes(length);
//            return new RowEvent(weight, iteration, new CompactRowData(bytes));
//        }

//        public void Serialize(in BinaryWriter writer, in RowEvent value)
//        {
//            writer.Write(value.Weight);
//            writer.Write(value.Iteration);
//            writer.Write(value.Length);
//            writer.Write(value.Span);
//        }
//    }
//}
