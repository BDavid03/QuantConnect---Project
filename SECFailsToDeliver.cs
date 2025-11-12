// Common/Data/Custom/SECFailsToDeliver.cs
using System;
using System.Globalization;
using System.IO;
using QuantConnect;
using QuantConnect.Data;
using NodaTime;
using NodaTime.Text;

namespace QuantConnect.Data.Custom
{
    public class SECFailsToDeliver : BaseData
    {
        private static readonly LocalDatePattern Ymd = LocalDatePattern.CreateWithInvariantCulture("yyyyMMdd");
        private static readonly DateTimeZone NyTz = DateTimeZoneProviders.Tzdb["America/New_York"];

        public string   Ticker         { get; private set; }
        public long     Quantity       { get; private set; }
        public decimal  Price          { get; private set; }
        public DateTime TransDateLocal { get; private set; }

        public override DateTime EndTime { get; set; }
        public override Resolution  DefaultResolution() => Resolution.Daily;
        public override bool        IsSparseData()      => true;
        public override DateTimeZone DataTimeZone()     => NyTz;

        // --- EXACT-DATE ROUTING: <data>\alternative\sec\failstodeliver\YYYYMMDD.zip#YYYYMMDD.csv
        private static string DayKey(DateTime dateUtc)
        {
            if (dateUtc.Kind != DateTimeKind.Utc)
                dateUtc = DateTime.SpecifyKind(dateUtc, DateTimeKind.Utc);
            var nyDate = Instant.FromDateTimeUtc(dateUtc).InZone(NyTz).Date;
            return nyDate.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        }

        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            var day = DayKey(date.Kind == DateTimeKind.Utc ? date : date.ToUniversalTime());
            var path = Path.Combine(
                Globals.DataFolder,
                "alternative", "sec", "failstodeliver",
                $"{day}.zip#{day}.csv"
            );
            return new SubscriptionDataSource(path, SubscriptionTransportMedium.LocalFile);
        }

        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            // Accepted row: YYYYMMDD,<ticker>,<quantity>,<price>  (no headers)
            if (string.IsNullOrWhiteSpace(line)) return null;

            var span = line.AsSpan().Trim();
            if (span.IsEmpty) return null;
            if (!char.IsDigit(span[0])) return null;

            string[] s;
            if (line.IndexOf('|') >= 0) s = line.Split('|');
            else if (line.IndexOf('\t') >= 0) s = line.Split('\t');
            else s = line.Split(',');

            if (s.Length < 4) return null;

            var dParse = Ymd.Parse(s[0].Trim());
            if (!dParse.Success) return null;

            var ticker = s[1].Trim();
            if (ticker.Length == 0) return null;

            // must match the subscribed label (your AddData<...>("ACNB"))
            if (!ticker.Equals(config.Symbol.Value, StringComparison.OrdinalIgnoreCase))
                return null;

            if (!long.TryParse(s[2].Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var qty))
                return null;

            if (qty <= 0) return null; // only real instances

            decimal px = 0m;
            var pstr = s[3].Trim();
            if (pstr.Length > 0 && !pstr.Equals("NA", StringComparison.OrdinalIgnoreCase))
            {
                if (!decimal.TryParse(pstr, NumberStyles.Number | NumberStyles.AllowExponent, CultureInfo.InvariantCulture, out px))
                    return null;
            }

            var ldt = dParse.Value.AtMidnight();
            var zdt = NyTz.AtLeniently(ldt);
            var utc = zdt.ToDateTimeUtc();

            return new SECFailsToDeliver
            {
                Symbol         = config.Symbol,
                Time           = utc,
                EndTime        = utc,
                Ticker         = ticker,
                Quantity       = qty,
                Price          = px,
                TransDateLocal = zdt.ToDateTimeUnspecified(),
                Value          = qty
            };
        }

        public override string ToString() => $"{Time:yyyy-MM-dd} {Ticker} qty={Quantity} px={Price}";
    }
}
