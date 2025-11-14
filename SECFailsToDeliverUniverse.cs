/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

using System;
using System.IO;
using System.Collections.Generic;
using System.Globalization;
using NodaTime;
using ProtoBuf;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.UniverseSelection;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// SEC Fails-To-Deliver universe data, one collection per date.
    /// Each line in the source file becomes a SECFailsToDeliver entry.
    /// </summary>
    [ProtoContract(SkipConstructor = true)]
    public class SECFailsToDeliverUniverse : BaseDataCollection
    {
        /// <summary>
        /// Time passed between the date of the data and the time the data became available to us.
        /// For daily FTD data we treat it as becoming available at the end of the settlement date.
        /// </summary>
        private static readonly TimeSpan _period = TimeSpan.FromDays(1);

        /// <summary>
        /// Time the data became available
        /// </summary>
        public override DateTime EndTime => Time + _period;

        /// <summary>
        /// Return the local file source for this universe.
        /// Expects: Data/alternative/sec/failstodeliver/yyyyMMdd.zip
        /// containing a single CSV named yyyyMMdd.csv with:
        /// DATE,SYMBOL,QUANTITY(FAILS),PRICE (no header).
        /// </summary>
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            var fileName = $"{date.ToStringInvariant(DateFormat.EightCharacter)}.zip";

            return new SubscriptionDataSource(
                Path.Combine(
                    Globals.DataFolder,
                    "alternative",
                    "sec",
                    "failstodeliver",
                    fileName
                ),
                SubscriptionTransportMedium.LocalFile,
                FileFormat.FoldingCollection
            );
        }

        /// <summary>
        /// Parses a single line from the zipped CSV into a SECFailsToDeliver instance.
        /// Reader is called once per line; the FoldingCollection file format
        /// then folds those lines into this BaseDataCollection.
        /// </summary>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            // CSV schema: DATE,SYMBOL,QUANTITY(FAILS),PRICE
            // Example:    20100917,GME,1234567,15.23
            var csv = line.Split(',');

            if (csv.Length < 2)
            {
                return null;
            }

            // DATE
            var settlementDate = QuantConnect.StringExtensions.Parse.DateTimeExact(csv[0], "yyyyMMdd");

            // SYMBOL
            var ticker = csv[1].Trim();
            if (string.IsNullOrEmpty(ticker))
            {
                return null;
            }

            var symbol = Symbol.Create(ticker, SecurityType.Equity, Market.USA);

            // QUANTITY (FAILS)
            long quantity = 0;
            if (csv.Length > 2 && !string.IsNullOrWhiteSpace(csv[2]))
            {
                long.TryParse(csv[2], NumberStyles.Any, CultureInfo.InvariantCulture, out quantity);
            }

            // PRICE
            decimal price = 0m;
            if (csv.Length > 3 && !string.IsNullOrWhiteSpace(csv[3]))
            {
                decimal.TryParse(csv[3], NumberStyles.Any, CultureInfo.InvariantCulture, out price);
            }

            // Time is start of the bar; EndTime (bar close) is settlementDate
            var barStartTime = settlementDate - _period;

            return new SECFailsToDeliver
            {
                Symbol   = symbol,
                Time     = barStartTime,
                Ticker   = ticker,
                Quantity = quantity,
                Price    = price,
                Value    = quantity
            };
        }

        /// <summary>
        /// Indicates whether the data is sparse.
        /// If true, we disable logging for missing files.
        /// </summary>
        public override bool IsSparseData()
        {
            return true;
        }

        /// <summary>
        /// Converts the instance to string
        /// </summary>
        public override string ToString()
        {
            return $"{Symbol} - Count: {Data?.Count ?? 0}";
        }

        /// <summary>
        /// Gets the default resolution for this data and security type
        /// </summary>
        public override Resolution DefaultResolution()
        {
            return Resolution.Daily;
        }

        /// <summary>
        /// Gets the supported resolution for this data and security type
        /// </summary>
        public override List<Resolution> SupportedResolutions()
        {
            return DailyResolution;
        }

        /// <summary>
        /// Specifies the data time zone for this data type.
        /// Settlement dates are in US market time.
        /// </summary>
        public override DateTimeZone DataTimeZone()
        {
            return DateTimeZoneProviders.Tzdb["America/New_York"];
        }

        /// <summary>
        /// Clones this instance
        /// </summary>
        public override BaseData Clone()
        {
            return new SECFailsToDeliverUniverse
            {
                Symbol = Symbol,
                Time   = Time,
                Data   = Data,
                Value  = Value
            };
        }
    }
}
