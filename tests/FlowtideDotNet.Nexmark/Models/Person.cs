// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

namespace FlowtideDotNet.Nexmark.Models;

/// <summary>
/// Represents a person in the NEXMark benchmark.
/// </summary>
public sealed class Person
{
    public required int Id { get; init; }
    public required string Name { get; init; }
    public required string EmailAddress { get; init; }
    public string? Phone { get; init; }
    public Address? Address { get; init; }
    public string? Homepage { get; init; }
    public string? CreditCard { get; init; }
    public Profile? Profile { get; init; }
    public IReadOnlyList<string>? Watches { get; init; }
    public required long DateTime { get; init; }
}
