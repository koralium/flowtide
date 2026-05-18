// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

namespace FlowtideDotNet.Nexmark.Models;

/// <summary>
/// Represents a person's profile in the NEXMark benchmark.
/// </summary>
public sealed class Profile
{
    public required string Income { get; init; }
    public string? Education { get; init; }
    public string? Gender { get; init; }
    public required string Business { get; init; }
    public int? Age { get; init; }
    public required IReadOnlyList<int> Interests { get; init; }
}
