// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

namespace FlowtideDotNet.Nexmark.Internal;

/// <summary>
/// Tracks synthetic time in seconds, advancing by a random increment each step.
/// Port of SimpleCalendar.java.
/// </summary>
internal sealed class SimpleCalendar
{
    private const int MaxIncrementSec = 60;
    private int _timeSec;
    private readonly JavaRandom _rnd;

    public SimpleCalendar(JavaRandom rnd)
    {
        _rnd = rnd;
        _timeSec = 0;
    }

    public int TimeInSecs => _timeSec;

    public int TimeInMs => _timeSec * 1000;

    public void IncrementTime()
    {
        _timeSec += _rnd.Next(MaxIncrementSec);
    }
}
