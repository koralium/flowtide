using FlowtideDotNet.Nexmark.Internal.Config;
using System;

namespace FlowtideDotNet.Nexmark.Models;

public struct Person
{
    public long Id { get; set; }
    public string Name { get; set; }
    public string EmailAddress { get; set; }
    public string CreditCard { get; set; }
    public string City { get; set; }
    public string State { get; set; }
    public DateTime DateTime { get; set; }
    public string Extra { get; set; }

    public static Person Generate(long eventId, long time, NexmarkConfig nex)
    {
        var rng = new Random((int)(eventId & 0xFFFFFFFF));
        long id = LastId(eventId, nex) + nex.FirstPersonId;
        string name = $"{rng.Choose(nex.FirstNames)} {rng.Choose(nex.LastNames)}";
        string emailAddress = $"{rng.GenString(7)}@{rng.GenString(5)}.com";
        string creditCard = $"{rng.Next(0, 10000):0000} {rng.Next(0, 10000):0000} {rng.Next(0, 10000):0000} {rng.Next(0, 10000):0000}";
        string city = rng.Choose(nex.UsCities);
        string state = rng.Choose(nex.UsStates);

        int currentSize = 8 + name.Length + emailAddress.Length + creditCard.Length + city.Length + state.Length;
        string extra = rng.GenNextExtra(currentSize, nex.AvgPersonByteSize);

        return new Person
        {
            Id = id,
            Name = name,
            EmailAddress = emailAddress,
            CreditCard = creditCard,
            City = city,
            State = state,
            DateTime = DateTimeOffset.FromUnixTimeMilliseconds(time).UtcDateTime,
            Extra = extra
        };
    }

    public static long NextId(long eventId, Random rng, NexmarkConfig nex)
    {
        long people = LastId(eventId, nex) + 1;
        long active = Math.Min(people, nex.ActivePeople);
        return people - active + rng.Next(0, (int)(active + nex.PersonIdLead));
    }

    public static long LastId(long eventId, NexmarkConfig nex)
    {
        long epoch = eventId / nex.ProportionDenominator;
        long offset = eventId % nex.ProportionDenominator;
        if (nex.PersonProportion <= offset)
        {
            offset = nex.PersonProportion - 1;
        }
        return epoch * nex.PersonProportion + offset;
    }
}
