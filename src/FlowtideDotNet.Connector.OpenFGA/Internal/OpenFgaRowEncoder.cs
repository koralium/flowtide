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
using OpenFga.Sdk.Model;
using System.Buffers;

namespace FlowtideDotNet.Connector.OpenFGA.Internal
{
    internal class OpenFgaRowEncoder
    {
        private static readonly FlxValue nullValue = FlxValue.FromBytes(FlexBuffer.Null());

        private readonly List<Action<TupleKey, int, FlxValue[]>> m_encoders;

        public OpenFgaRowEncoder(List<Action<TupleKey, int, FlxValue[]>> encoders)
        {
            m_encoders = encoders;
        }

        public RowEvent Encode(TupleKey tupleKey, int weight)
        {
            FlxValue[] arr = new FlxValue[m_encoders.Count];
            for (int i = 0; i < m_encoders.Count; i++)
            {
                m_encoders[i](tupleKey, i, arr);
            }
            var row = new ArrayRowData(arr);

            return new RowEvent(weight, 0, row);
        }

        public static OpenFgaRowEncoder Create(List<string> names)
        {
            var encoders = new List<Action<TupleKey, int, FlxValue[]>>();
            var flexBuffer = new FlexBuffer(ArrayPool<byte>.Shared);
            Dictionary<string, FlxValue> typesAndRelationValues = new Dictionary<string, FlxValue>();

            for (int i = 0; i < names.Count; i++)
            {
                var name = names[i];
                switch (name.ToLower())
                {
                    case "user_type":
                        encoders.Add((tupleKey, i, arr) =>
                        {
                            var typeName = tupleKey.User.Substring(0, tupleKey.User.IndexOf(':'));
                            if (typesAndRelationValues.TryGetValue(typeName, out var value))
                            {
                                arr[i] = value;
                                return;
                            }
                            flexBuffer.NewObject();
                            flexBuffer.Add(typeName);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            typesAndRelationValues.Add(typeName, flxValue);
                            arr[i] = flxValue;
                        });
                        break;
                    case "user_id":
                        encoders.Add((tupleKey, i, arr) =>
                        {
                            flexBuffer.NewObject();
                            var startIndex = tupleKey.User.IndexOf(':') + 1;
                            var hashTagLocation = tupleKey.User.LastIndexOf('#');
                            if (hashTagLocation > 0)
                            {
                                flexBuffer.Add(tupleKey.User.Substring(startIndex, hashTagLocation - startIndex));
                            }
                            else
                            {
                                flexBuffer.Add(tupleKey.User.Substring(startIndex));
                            }
                            arr[i] = FlxValue.FromBytes(flexBuffer.Finish());
                        });
                        break;
                    case "user_relation":
                        encoders.Add((tupleKey, i, arr) =>
                        {
                            var userRelationIndex = tupleKey.User.LastIndexOf('#');
                            if (userRelationIndex > 0)
                            {
                                var userRelation = tupleKey.User.Substring(userRelationIndex + 1);
                                if (typesAndRelationValues.TryGetValue(userRelation, out var value))
                                {
                                    arr[i] = value;
                                    return;
                                }
                                flexBuffer.NewObject();
                                flexBuffer.Add(userRelation);
                                var bytes = flexBuffer.Finish();
                                var flxValue = FlxValue.FromBytes(bytes);
                                typesAndRelationValues.Add(userRelation, flxValue);
                                arr[i] = flxValue;
                            }
                            else
                            {
                                arr[i] = nullValue;
                            }
                        });
                        break;
                    case "relation":
                        encoders.Add((tupleKey, i, arr) =>
                        {
                            if (typesAndRelationValues.TryGetValue(tupleKey.Relation, out var value))
                            {
                                arr[i] = value;
                                return;
                            }
                            flexBuffer.NewObject();
                            flexBuffer.Add(tupleKey.Relation);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            typesAndRelationValues.Add(tupleKey.Relation, flxValue);
                            arr[i] = flxValue;
                        });
                        break;
                    case "object_id":
                        encoders.Add((tupleKey, i, arr) =>
                        {
                            flexBuffer.NewObject();
                            flexBuffer.Add(tupleKey.Object.Substring(tupleKey.Object.IndexOf(':') + 1));
                            arr[i] = FlxValue.FromBytes(flexBuffer.Finish());
                        });
                        break;
                    case "object_type":
                        encoders.Add((tupleKey, i, arr) =>
                        {
                            var objectTypeValue = tupleKey.Object.Substring(0, tupleKey.Object.IndexOf(':'));
                            if (typesAndRelationValues.TryGetValue(objectTypeValue, out var value))
                            {
                                arr[i] = value;
                                return;
                            }
                            flexBuffer.NewObject();
                            flexBuffer.Add(objectTypeValue);
                            var bytes = flexBuffer.Finish();
                            var flxValue = FlxValue.FromBytes(bytes);
                            typesAndRelationValues.Add(objectTypeValue, flxValue);
                            arr[i] = flxValue;
                        });
                        break;
                }
            }

            return new OpenFgaRowEncoder(encoders);
        }
    }
}
