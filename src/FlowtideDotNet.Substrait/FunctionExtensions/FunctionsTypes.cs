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

namespace FlowtideDotNet.Substrait.FunctionExtensions;

/// <summary>Flowtide's user-defined type identities in Substrait plans.</summary>
public static class FunctionsTypes
{
    public const string Urn = "extension:io.github.koralium.flowtide:types";
    public const string Dynamic = "dynamic";
    public const string NamedStruct = "named_struct";
}
