// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

namespace FlowtideDotNet.Nexmark.Internal;

/// <summary>
/// Manages person ID allocation and random existing-ID selection.
/// Port of Persons.java.
/// </summary>
internal sealed class PersonIdManager
{
    public const int PersonDistrSize = 100;

    private int _highChunk = 1;
    private int _currId;
    private readonly JavaRandom _rnd;

    public PersonIdManager(JavaRandom rnd)
    {
        _rnd = rnd;
    }

    /// <summary>
    /// Allocates and returns a new person ID.
    /// </summary>
    public int GetNewId()
    {
        int newId = _currId;
        _currId++;
        if (newId == _highChunk * PersonDistrSize)
        {
            _highChunk++;
        }
        return newId;
    }

    /// <summary>
    /// Returns a random existing person ID.
    /// </summary>
    public int GetExistingId()
    {
        int id = _rnd.Next(PersonDistrSize);
        id += GetRandomChunkOffset();
        return id % _currId;
    }

    private int GetRandomChunkOffset()
    {
        int chunkId = _rnd.Next(_highChunk);
        return chunkId * PersonDistrSize;
    }
}
