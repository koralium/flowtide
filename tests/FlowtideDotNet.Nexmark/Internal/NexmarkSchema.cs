using FlowtideDotNet.Substrait.Type;
using System.Collections.Generic;

namespace FlowtideDotNet.Nexmark.Internal;

public static class NexmarkSchema
{
    public static NamedStruct PersonSchema => new NamedStruct
    {
        Names = new List<string> { "id", "name", "emailAddress", "creditCard", "city", "state", "dateTime", "extra" },
        Struct = new Struct
        {
            Types = new List<SubstraitBaseType>
            {
                new Int64Type(), // id
                new StringType(), // name
                new StringType(), // emailAddress
                new StringType(), // creditCard
                new StringType(), // city
                new StringType(), // state
                new StringType(), // dateTime
                new StringType()  // extra
            }
        }
    };

    public static NamedStruct AuctionSchema => new NamedStruct
    {
        Names = new List<string> { "id", "itemName", "description", "initialBid", "reserve", "dateTime", "expires", "seller", "category", "extra" },
        Struct = new Struct
        {
            Types = new List<SubstraitBaseType>
            {
                new Int64Type(), // id
                new StringType(), // itemName
                new StringType(), // description
                new Int64Type(), // initialBid
                new Int64Type(), // reserve
                new StringType(), // dateTime
                new StringType(), // expires
                new Int64Type(), // seller
                new Int64Type(), // category
                new StringType()  // extra
            }
        }
    };

    public static NamedStruct BidSchema => new NamedStruct
    {
        Names = new List<string> { "auction", "bidder", "price", "channel", "url", "date_time", "extra" },
        Struct = new Struct
        {
            Types = new List<SubstraitBaseType>
            {
                new Int64Type(), // auction
                new Int64Type(), // bidder
                new Int64Type(), // price
                new StringType(), // channel
                new StringType(), // url
                new StringType(), // dateTime
                new StringType()  // extra
            }
        }
    };
}
