using System;
using System.Collections.Generic;
using System.Text;

namespace FlowtideDotNet.Nexmark.Config;

public static class NexmarkUtils
{
    private const int MIN_STRING_LENGTH = 3;

    public static string GenString(this Random self, int max)
    {
        return self.GenStringWithDelimiter(max, " ");
    }

    public static string GenStringWithDelimiter(this Random self, int max, string delimiter)
    {
        int len = self.Next(MIN_STRING_LENGTH, max);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++)
        {
            if (self.Next(0, 13) == 0)
            {
                sb.Append(delimiter);
            }
            else
            {
                char c = (char)('a' + self.Next(0, 26));
                sb.Append(c);
            }
        }
        return sb.ToString().Trim();
    }

    public static string GenExactString(this Random self, int length)
    {
        StringBuilder s = new StringBuilder();
        int rnd = 0;
        int n = 0;
        for (int i = 0; i < length; i++)
        {
            if (n == 0)
            {
                rnd = self.Next(); // Random Int32
                n = 6; // log_26(2^31)
            }
            char c = (char)('a' + (rnd % 26));
            s.Append(c);
            rnd /= 26;
            n -= 1;
        }
        return s.ToString();
    }

    public static string GenNextExtra(this Random self, int currentSize, int desiredAverageSize)
    {
        if (currentSize > desiredAverageSize)
        {
            return string.Empty;
        }
        int desiredAv = desiredAverageSize - currentSize;
        int delta = (int)Math.Round(desiredAv * 0.2);
        int minSize = desiredAv - delta;
        int desiredSize = minSize + (delta == 0 ? 0 : self.Next(0, 2 * delta));
        return self.GenExactString(desiredSize);
    }

    public static long GenPrice(this Random self)
    {
        double val = self.NextDouble();
        return (long)Math.Round(Math.Pow(10.0, val * 6.0) * 100.0);
    }

    public static string MilliTsToTimestampString(long milliTs)
    {
        var dt = DateTimeOffset.FromUnixTimeMilliseconds(milliTs).UtcDateTime;
        return dt.ToString("yyyy-MM-dd HH:mm:ss.fff");
    }

    public static string GetBaseUrl(ulong seed)
    {
        var rng = new Random((int)(seed & 0xFFFFFFFF));
        string id0 = rng.GenStringWithDelimiter(5, "_");
        string id1 = rng.GenStringWithDelimiter(5, "_");
        string id2 = rng.GenStringWithDelimiter(5, "_");
        return $"https://www.nexmark.com/{id0}/{id1}/{id2}/item.htm?query=1";
    }

    public static Dictionary<int, (string Channel, string Url)> BuildChannelUrlMap(int channelNumber)
    {
        var ans = new Dictionary<int, (string Channel, string Url)>();
        for (int i = 0; i < channelNumber; i++)
        {
            string url = GetBaseUrl((ulong)i);
            var rng = new Random(i);
            if (rng.Next(0, 10) > 0)
            {
                url += "&channel_id=" + Math.Abs((long)ReverseBits(i));
            }
            string channel = $"channel-{i}";
            ans[i] = (channel, url);
        }
        return ans;
    }

    private static int ReverseBits(int n)
    {
        uint v = (uint)n;
        v = ((v >> 1) & 0x55555555) | ((v & 0x55555555) << 1);
        v = ((v >> 2) & 0x33333333) | ((v & 0x33333333) << 2);
        v = ((v >> 4) & 0x0F0F0F0F) | ((v & 0x0F0F0F0F) << 4);
        v = ((v >> 8) & 0x00FF00FF) | ((v & 0x00FF00FF) << 8);
        v = (v >> 16) | (v << 16);
        return (int)v;
    }
    
    public static T Choose<T>(this Random self, IList<T> list)
    {
        int idx = self.Next(0, list.Count);
        return list[idx];
    }
}
