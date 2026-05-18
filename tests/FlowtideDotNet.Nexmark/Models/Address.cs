// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

namespace FlowtideDotNet.Nexmark.Models;

/// <summary>
/// Represents a person's address in the NEXMark benchmark.
/// </summary>
public sealed class Address
{
    public required string Street { get; init; }
    public required string City { get; init; }
    public required string Country { get; init; }
    public required string Province { get; init; }
    public required string Zipcode { get; init; }
}
