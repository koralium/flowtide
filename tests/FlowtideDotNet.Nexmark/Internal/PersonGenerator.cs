// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using FlowtideDotNet.Nexmark.Data;
using FlowtideDotNet.Nexmark.Models;

namespace FlowtideDotNet.Nexmark.Internal;

/// <summary>
/// Generates random person field values.
/// Port of PersonGen.java.
/// </summary>
internal sealed class PersonGenerator
{
    public const int NumCategories = 1000;

    private readonly JavaRandom _rnd;

    public PersonGenerator(JavaRandom rnd)
    {
        _rnd = rnd;
    }

    /// <summary>
    /// Generates a new Person object with random field values.
    /// </summary>
    public Person Generate(int personId, AuctionManager auctions, long dateTime)
    {
        int ifn = _rnd.Next(FirstNames.Count);
        int iln = _rnd.Next(LastNames.Count);
        int iem = _rnd.Next(ReferenceData.NumEmails);

        string name = $"{FirstNames.Values[ifn]} {LastNames.Values[iln]}";
        string email = $"{LastNames.Values[iln]}@{ReferenceData.Emails[iem]}";

        string? phone = null;
        if (_rnd.NextBoolean()) // nextBoolean()
        {
            phone = $"+{_rnd.Next(98) + 1}({_rnd.Next(989) + 10}){_rnd.Next(9864196) + 123457}";
        }

        Address? address = null;
        if (_rnd.NextBoolean())
        {
            address = GenAddress();
        }

        string? homepage = null;
        if (_rnd.NextBoolean())
        {
            homepage = $"http://www.{ReferenceData.Emails[iem]}/~{LastNames.Values[iln]}";
        }

        string? creditCard = null;
        if (_rnd.NextBoolean())
        {
            creditCard = $"{_rnd.Next(9000) + 1000} {_rnd.Next(9000) + 1000} {_rnd.Next(9000) + 1000} {_rnd.Next(9000) + 1000}";
        }

        Profile? profile = null;
        if (_rnd.NextBoolean())
        {
            profile = GenProfile();
        }

        // watches skipped, matching the Java implementation
        // (too expensive and problematic for persons generated before items)

        return new Person
        {
            Id = personId,
            Name = name,
            EmailAddress = email,
            Phone = phone,
            Address = address,
            Homepage = homepage,
            CreditCard = creditCard,
            Profile = profile,
            Watches = null,
            DateTime = dateTime
        };
    }

    private Address GenAddress()
    {
        int ist = _rnd.Next(LastNames.Count);
        int ict = _rnd.Next(ReferenceData.NumCities);
        int icn = (_rnd.Next(4) != 0) ? 0 : _rnd.Next(ReferenceData.NumCountries);
        int ipv = (icn == 0)
            ? _rnd.Next(ReferenceData.NumProvinces)
            : _rnd.Next(LastNames.Count);

        string street = $"{_rnd.Next(99) + 1} {LastNames.Values[ist]} St";
        string city = ReferenceData.Cities[ict];

        string country;
        string province;
        if (icn == 0)
        {
            country = "United States";
            province = ReferenceData.Provinces[ipv];
        }
        else
        {
            country = ReferenceData.Countries[icn];
            province = LastNames.Values[ipv];
        }

        string zipcode = (_rnd.Next(99999) + 1).ToString();

        return new Address
        {
            Street = street,
            City = city,
            Country = country,
            Province = province,
            Zipcode = zipcode
        };
    }

    private Profile GenProfile()
    {
        string? education = null;
        if (_rnd.NextBoolean())
        {
            education = ReferenceData.Education[_rnd.Next(ReferenceData.NumEducation)];
        }

        string? gender = null;
        if (_rnd.NextBoolean())
        {
            gender = (_rnd.NextBoolean()) ? "male" : "female";
        }

        string business = (_rnd.NextBoolean()) ? "Yes" : "No";

        int? age = null;
        if (_rnd.NextBoolean())
        {
            age = _rnd.Next(15) + 30;
        }

        int incomeWhole = _rnd.Next(30000) + 40000;
        int incomeCents = _rnd.Next(99);
        string income = $"{incomeWhole}.{incomeCents}";

        int cInterest = _rnd.Next(5);
        var interests = new List<int>(cInterest);
        for (int i = 0; i < cInterest; i++)
        {
            interests.Add(_rnd.Next(NumCategories));
        }

        return new Profile
        {
            Income = income,
            Education = education,
            Gender = gender,
            Business = business,
            Age = age,
            Interests = interests
        };
    }
}
