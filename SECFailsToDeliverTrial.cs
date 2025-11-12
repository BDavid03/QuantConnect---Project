// Algorithm.CSharp/SECFailsToDeliverTrial.cs
using System;
using System.Collections.Generic;
using System.Linq;
using QuantConnect;
using QuantConnect.Algorithm;
using QuantConnect.Data;
using QuantConnect.Data.Custom;
using QuantConnect.Data.Market;
using QuantConnect.Securities;

namespace QuantConnect.Algorithm.CSharp
{
    public class SECFailsToDeliverTrial : QCAlgorithm
    {
        private const string TICK = "ACNB";  // change here to test another symbol

        private Symbol _eq;
        private Symbol _ftd;
        private readonly List<(DateTime t, long q, decimal px)> _hits = new();

        public override void Initialize()
        {
            // Narrow the window to around the known instance 2010-10-01
            SetStartDate(2010, 9, 20);
            SetEndDate  (2010, 10, 15);
            SetCash(100_000);

            SetSecurityInitializer(s =>
            {
                if (s.Type == SecurityType.Equity)
                    s.SetDataNormalizationMode(DataNormalizationMode.Raw);
            });

            _eq  = AddEquity(TICK, Resolution.Daily, Market.USA).Symbol;
            _ftd = AddData<SECFailsToDeliver>(TICK, Resolution.Daily).Symbol;

            var chart = new Chart("FTD");
            chart.AddSeries(new Series("Qty", SeriesType.Line, 0));
            chart.AddSeries(new Series("Px",  SeriesType.Line, 1));
            AddChart(chart);

            // History only returns what Reader() actually emits (qty>0)
            var hist = History<SECFailsToDeliver>(_ftd, StartDate, EndDate).ToList();
            Debug($"FTD[{TICK}] historical rows in range: {hist.Count}");
            foreach (var r in hist)
                Debug($"FTD[{TICK}] {r.Time:yyyy-MM-dd} qty={r.Quantity} px={r.Price}");
        }

        public override void OnData(Slice slice)
        {
            if (!slice.ContainsKey(_ftd)) return;

            var r = slice.Get<SECFailsToDeliver>(_ftd);
            _hits.Add((r.Time, r.Quantity, r.Price));

            Plot("FTD", "Qty", (double)r.Value);
            Plot("FTD", "Px",  (double)r.Price);

            // Print each recognized instance
            Debug($"FTD[{TICK}] {r.Time:yyyy-MM-dd} qty={r.Quantity} px={r.Price}");
        }

        public override void OnEndOfAlgorithm()
        {
            if (_hits.Count == 0)
            {
                Debug($"FTD[{TICK}] no recognized rows in this window.");
                return;
            }
            var first = _hits.Min(x => x.t);
            var last  = _hits.Max(x => x.t);
            var totalQty = _hits.Sum(x => x.q);
            var avgPx = _hits.Count(x => x.px != 0m) > 0 ? _hits.Where(x => x.px != 0m).Average(x => x.px) : 0m;

            Debug($"FTD[{TICK}] recognized rows: {_hits.Count} | first={first:yyyy-MM-dd} last={last:yyyy-MM-dd}");
            foreach (var x in _hits.OrderBy(x => x.t))
                Debug($"  {x.t:yyyy-MM-dd} qty={x.q} px={x.px}");
            Debug($"FTD[{TICK}] totals: rows={_hits.Count}, sumQty={totalQty}, avgPx(nonzero)={avgPx:F4}");
        }
    }
}
