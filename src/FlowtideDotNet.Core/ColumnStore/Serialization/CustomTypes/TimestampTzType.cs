﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using Apache.Arrow.Types;

namespace FlowtideDotNet.Core.ColumnStore.Serialization.CustomTypes
{
    internal class TimestampTzType : FixedSizeBinaryType, ICustomArrowType
    {
        public const string TypeName = "TimestampTz";
        public const string ExtensionName = "flowtide.timestamptz";
        public static readonly TimestampTzType Default = new TimestampTzType();

        public TimestampTzType() : base(16)
        {
        }

        public override string Name => TypeName;

        public string CustomTypeName => ExtensionName;
    }
}
