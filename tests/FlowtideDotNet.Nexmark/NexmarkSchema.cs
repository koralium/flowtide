// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Nexmark;

public static class NexmarkSchema
{
    public static NamedStruct PersonSchema => new NamedStruct
    {
        Names = new List<string> { "id", "name", "emailAddress", "phone", "address", "homepage", "creditCard", "profile", "dateTime" },
        Struct = new Struct
        {
            Types = new List<SubstraitBaseType>
            {
                new Int64Type(), // id (Int64)
                new StringType(), // name (String)
                new StringType(), // emailAddress (String)
                new StringType(), // phone (String)
                new NamedStruct()
                {
                    Names = new List<string> { "street", "city", "country", "province", "zipcode" },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>
                        {
                            new StringType(), // street
                            new StringType(), // city
                            new StringType(), // country
                            new StringType(), // province
                            new StringType()  // zipcode
                        }
                    }
                }, // address (Struct)
                new StringType(), // homepage (String)
                new StringType(), // creditCard (String)
                new NamedStruct()
                {
                    Names = new List<string> { "income", "education", "gender", "business", "age", "interests" },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>
                        {
                            new StringType(), // income
                            new StringType(), // education
                            new StringType(), // gender
                            new StringType(), // business
                            new Int64Type(), // age
                            new ListType(new Int64Type()) // interests
                        }
                    }
                }, // profile (Struct)
                new Int64Type()  // dateTime (Int64)
            }
        }
    };

    public static NamedStruct AuctionSchema => new NamedStruct
    {
        Names = new List<string> { "id", "itemId", "sellerId", "category", "initialPrice", "reserve", "privacy", "quantity", "type", "startTime", "endTime", "dateTime" },
        Struct = new Struct
        {
            Types = new List<SubstraitBaseType>
            {
                new Int64Type(), // id
                new Int64Type(), // itemId
                new Int64Type(), // sellerId
                new Int64Type(), // category
                new Int64Type(), // initialPrice
                new Int64Type(), // reserve
                new StringType(), // privacy
                new Int64Type(), // quantity
                new StringType(), // type
                new Int64Type(), // startTime
                new Int64Type(), // endTime
                new Int64Type()  // dateTime
            }
        }
    };

    public static NamedStruct BidSchema => new NamedStruct
    {
        Names = new List<string> { "auctionId", "bidderId", "price", "dateTime" },
        Struct = new Struct
        {
            Types = new List<SubstraitBaseType>
            {
                new Int64Type(), // auctionId
                new Int64Type(), // bidderId
                new Int64Type(), // price
                new Int64Type()  // dateTime
            }
        }
    };
}
