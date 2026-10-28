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

namespace FlowtideDotNet.Substrait;

/// <summary>Validates Substrait extension identifiers of the form extension:OWNER:ID.</summary>
public static class ExtensionUrn
{
    /// <summary>Returns the identifier, or throws when it is not an extension URN.</summary>
    /// <param name="urn">The extension namespace identifier.</param>
    /// <returns>The validated identifier.</returns>
    public static string Validate(string urn)
    {
        ArgumentNullException.ThrowIfNull(urn);
        var parts = urn.Split(':');
        if (parts.Length != 3 || parts[0] != "extension" || parts[1].Length == 0 || parts[2].Length == 0
            || urn.Any(char.IsWhiteSpace))
        {
            throw new ArgumentException("Extension identifiers must use extension:<OWNER>:<ID>.", nameof(urn));
        }
        return urn;
    }
}
