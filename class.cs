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

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Single SEC Fails-To-Deliver record:
    /// DATE,SYMBOL,QUANTITY(FAILS),PRICE
    /// </summary>
    [ProtoContract(SkipConstructor = true)]
    public class SECFailsToDeliver : BaseData
    {
        /// <summary>
        /// Underlying equity ticker (e.g. GME)
        /// </summary>
        [ProtoMember(2000)]
        public string Ticker { get; set; }

        /// <summary>
        /// Quantity of fails for this date/symbol
        /// </summary>
        [ProtoMember(2001)]
        public long Quantity { get; set; }

        /// <summary>
        /// Reference price for the security on this date
        /// </summary>
        [ProtoMember(2002)]
        public decimal Price { get; set; }

        /// <summary>
        /// Time passed between the date of the data and the time the data became available to us
        /// </summary>
        public TimeSpan Period { get; set; } = TimeSpan.FromDays(1);

        /// <summary>
        /// Time the data became available (end of settlement date)
        /// </summary>
        public override DateTime EndTime => Time + Period;

        /// <summary>
        /// Local file source for a single FTD "bar".
        /// Expects: Data/alternative/sec/failstodeliver/yyyyMMdd.zip
        /// containing yyyyMMdd.csv with lines:
        /// DATE,SYMBOL,QUANTITY(FAILS),PRICE
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
                SubscriptionTransportMedium.LocalFile
            );
        }

        /// <summary>
        /// Parses the data from one CSV line into a SECFailsToDeliver instance.
        /// </summary>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            // CSV schema: DATE,SYMBOL,QUANTITY(FAILS),PRICE
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

            var barStartTime = settlementDate - Period;

            return new SECFailsToDeliver
            {
                Symbol   = symbol,
                Time     = barStartTime,
                Ticker   = ticker,
                Quantity = quantity,
                Price    = price,
                Value    = quantity,   // use fails count as series value by default
                Period   = Period
            };
        }

        /// <summary>
        /// Clone
        /// </summary>
        public override BaseData Clone()
        {
            return new SECFailsToDeliver
            {
                Symbol   = Symbol,
                Time     = Time,
                Ticker   = Ticker,
                Quantity = Quantity,
                Price    = Price,
                Value    = Value,
                Period   = Period
            };
        }

        /// <summary>
        /// Indicates whether the data source is tied to an underlying symbol
        /// and requires mapping (corporate actions).
        /// </summary>
        public override bool RequiresMapping()
        {
            return true;
        }

        /// <summary>
        /// Indicates whether the data is sparse.
        /// If true, we disable logging for missing files
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
            return $"{Symbol} - {Ticker} - {Quantity} @ {Price}";
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
        /// Specifies the data time zone for this data type
        /// </summary>
        public override DateTimeZone DataTimeZone()
        {
            return DateTimeZoneProviders.Tzdb["America/New_York"];
        }
    }
}
