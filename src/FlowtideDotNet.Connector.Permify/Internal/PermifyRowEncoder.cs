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
using FlowtideDotNet.Core;
using System.Buffers;
using PermifyProto = Base.V1;

namespace FlowtideDotNet.Connector.Permify.Internal
{
    internal class PermifyRowEncoder
    {
        private readonly List<Action<PermifyProto.Tuple, int, FlxValue[]>> m_encoders;

        public PermifyRowEncoder(List<Action<PermifyProto.Tuple, int, FlxValue[]>> encoders)
        {
            this.m_encoders = encoders;
        }

        public RowEvent Encode(PermifyProto.Tuple relationship, int weight)
        {
            FlxValue[] values = new FlxValue[m_encoders.Count];
            for (int i = 0; i < m_encoders.Count; i++)
            {
                m_encoders[i](relationship, i, values);
            }
            return new RowEvent(weight, 0, new ArrayRowData(values));
        }

        public static PermifyRowEncoder Create(List<string> names)
        {
            List<Action<PermifyProto.Tuple, int, FlxValue[]>> encoders = new List<Action<PermifyProto.Tuple, int, FlxValue[]>>();
            Dictionary<string, FlxValue> typesAndRelationValues = new Dictionary<string, FlxValue>();
            FlexBuffer flexBuffer = new FlexBuffer(ArrayPool<byte>.Shared);
            for (int i = 0; i < names.Count; i++)
            {
                var name = names[i];
                switch (name.ToLower())
                {
                    case "subject_type":
                        encoders.Add((r, i, v) =>
                        {
                            if (typesAndRelationValues.TryGetValue(r.Subject.Type, out var value))
                            {
                                v[i] = value;
                                return;
                            }
                            flexBuffer.NewObject();
                            flexBuffer.Add(r.Subject.Type);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            typesAndRelationValues.Add(r.Subject.Type, flxValue);
                            v[i] = flxValue;
                        });
                        break;
                    case "subject_id":
                        encoders.Add((r, i, v) =>
                        {
                            flexBuffer.NewObject();
                            flexBuffer.Add(r.Subject.Id);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            v[i] = flxValue;
                        });
                        break;
                    case "subject_relation":
                        encoders.Add((r, i, v) =>
                        {
                            if (typesAndRelationValues.TryGetValue(r.Subject.Relation, out var value))
                            {
                                v[i] = value;
                                return;
                            }
                            flexBuffer.NewObject();
                            flexBuffer.Add(r.Subject.Relation);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            typesAndRelationValues.Add(r.Subject.Relation, flxValue);
                            v[i] = flxValue;
                        });
                        break;
                    case "relation":
                        encoders.Add((r, i, v) =>
                        {
                            if (typesAndRelationValues.TryGetValue(r.Relation, out var value))
                            {
                                v[i] = value;
                                return;
                            }
                            flexBuffer.NewObject();
                            flexBuffer.Add(r.Relation);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            typesAndRelationValues.Add(r.Relation, flxValue);
                            v[i] = flxValue;
                        });
                        break;
                    case "entity_type":
                        encoders.Add((r, i, v) =>
                        {
                            if (typesAndRelationValues.TryGetValue(r.Entity.Type, out var value))
                            {
                                v[i] = value;
                                return;
                            }
                            flexBuffer.NewObject();
                            flexBuffer.Add(r.Entity.Type);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            typesAndRelationValues.Add(r.Entity.Type, flxValue);
                            v[i] = flxValue;
                        });
                        break;
                    case "entity_id":
                        encoders.Add((r, i, v) =>
                        {
                            flexBuffer.NewObject();
                            flexBuffer.Add(r.Entity.Id);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            v[i] = flxValue;
                        });
                        break;
                }
            }
            return new PermifyRowEncoder(encoders);
        }
    }
}
