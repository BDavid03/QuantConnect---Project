// Algorithm.CSharp/SECFailsToDeliverSmokeTest.cs
using System.Linq;
using QuantConnect;
using QuantConnect.Algorithm;
using QuantConnect.Data;
using QuantConnect.Data.Custom;

namespace QuantConnect.Algorithm.CSharp
{
    public class SECFailsToDeliverSmokeTest : QCAlgorithm
    {
        private Symbol _ftd; // label-only custom symbol (not equity)

        public override void Initialize()
        {
            // Use dates that actually correspond to bundles you wrote (e.g., 20100115.zip, 20100131.zip)
            SetStartDate(2010, 1, 1);
            SetEndDate(2010, 2, 28);
            SetCash(100000);

            // Subscribe to the custom stream with any label; file routing ignores this label
            _ftd = AddData<SECFailsToDeliver>("ALL", Resolution.Daily).Symbol;

            // If you want equity mapping separately, you can also do:
            // var aapl = AddEquity("AAPL", Resolution.Daily).Symbol;
        }

        public override void OnData(Slice slice)
        {
            // Don’t index by symbol: rows are not per-symbol bars.
            var rows = slice.Values.OfType<SECFailsToDeliver>().Take(10).ToList();
            if (rows.Count == 0) return;

            foreach (var r in rows)
                Log($"FTD {r.Time:yyyy-MM-dd} {r.Ticker} qty={r.Quantity} px={r.Price}");
        }
    }
}
