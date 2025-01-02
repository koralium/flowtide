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

using Authzed.Api.V1;
using FlexBuffers;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.SpiceDB.Internal
{
    internal class ColumnSpiceDbRowEncoder
    {
        private readonly List<Action<Relationship, int, Column[]>> m_encoders;

        public ColumnSpiceDbRowEncoder(List<Action<Relationship, int, Column[]>> encoders)
        {
            this.m_encoders = encoders;
        }

        public void Encode(Relationship relationship, Column[] columns)
        {
            for (int i = 0; i < m_encoders.Count; i++)
            {
                m_encoders[i](relationship, i, columns);
            }
        }

        public static ColumnSpiceDbRowEncoder Create(List<string> names)
        {
            List<Action<Relationship, int, Column[]>> encoders = new List<Action<Relationship, int, Column[]>>();
            //Dictionary<string, FlxValue> typesAndRelationValues = new Dictionary<string, FlxValue>();
            for (int i = 0; i < names.Count; i++)
            {
                var name = names[i];
                switch (name.ToLower())
                {
                    case "subject_type":
                        encoders.Add((r, i, v) =>
                        {
                            v[i].Add(new StringValue(r.Subject.Object.ObjectType));
                        });
                        break;
                    case "subject_id":
                        encoders.Add((r, i, v) =>
                        {
                            v[i].Add(new StringValue(r.Subject.Object.ObjectId));
                        });
                        break;
                    case "subject_relation":
                        encoders.Add((r, i, v) =>
                        {
                            if (r.Subject.OptionalRelation == null)
                            {
                                v[i].Add(NullValue.Instance);
                                return;
                            }
                            v[i].Add(new StringValue(r.Subject.OptionalRelation));
                        });
                        break;
                    case "relation":
                        encoders.Add((r, i, v) =>
                        {
                            v[i].Add(new StringValue(r.Relation));
                        });
                        break;
                    case "resource_type":
                        encoders.Add((r, i, v) =>
                        {
                            v[i].Add(new StringValue(r.Resource.ObjectType));
                        });
                        break;
                    case "resource_id":
                        encoders.Add((r, i, v) =>
                        {
                            v[i].Add(new StringValue(r.Resource.ObjectId));
                        });
                        break;
                }
            }
            return new ColumnSpiceDbRowEncoder(encoders);
        }

    }
}
