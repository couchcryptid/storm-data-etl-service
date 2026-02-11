// Package domain models National Weather Service (NWS) storm report data.
//
// # Data Source
//
// Storm reports originate from the NOAA Storm Prediction Center (SPC) daily
// CSV files, available at https://www.spc.noaa.gov/climo/reports/. The upstream
// collector service fetches these CSVs on a cron schedule, parses them, injects
// an "EventType" field, and publishes each row as flat JSON to the Kafka source topic.
//
// # NWS Data Conventions
//
// Location format:
//
//	"<distance> <compass> <place>"  →  e.g. "8 ESE Chappel"
//	means 8 miles East-Southeast of Chappel, TX.
//	Compass directions: N, S, E, W, NE, NW, SE, SW, NNE, ENE, ESE, SSE, SSW, WSW, WNW, NNW.
//	Reports at the named location omit distance and direction (e.g. just "Ravenna").
//
// Time format:
//
//	HHMM in 24-hour notation, e.g. "1510" = 15:10 UTC.
//	Three-digit values are zero-padded: "930" → "0930".
//	The date portion comes from the Kafka message timestamp (set by the collector
//	from the CSV filename date). Combined to produce a full UTC time.
//
// Magnitude encoding (varies by event type, sometimes inconsistent in source data):
//
//	Hail ("Size" column):
//	  - Inches as a decimal: 1.25 = 1.25 inches
//	  - Hundredths of inches (legacy): 125 = 1.25 inches
//	  - Heuristic: values ≥ 10 with unit "in" are assumed to be hundredths
//	    because the largest hail ever recorded in the US was ~8 inches.
//	Tornado ("F_Scale" column):
//	  - Enhanced Fujita scale integer: 0–5 (EF0 through EF5)
//	  - May include "EF" or "F" prefix, which is stripped during parsing.
//	Wind ("Speed" column):
//	  - Miles per hour as an integer: 65 = 65 mph
//
// Unknown values:
//
//	"UNK" is the NOAA sentinel for unknown or unreported magnitude.
//	Empty strings are treated as zero (unmeasured).
//
// NWS office codes:
//
//	Weather Forecast Office (WFO) codes appear in parentheses at the end of
//	comment strings: "Large hail reported. (OUN)" → office code "OUN" (Norman, OK).
//	Codes are 3–5 uppercase letters. Extracted by [extractSourceOffice].
//
// Severity classification:
//
//	Derived from magnitude using thresholds informed by NWS Severe Weather Criteria
//	and the Enhanced Fujita Scale. The four-level scale (minor, moderate, severe,
//	extreme) is a project-specific simplification for user-facing queries:
//
//	  Hail:    <0.75" minor | <1.5" moderate | <2.5" severe | ≥2.5" extreme
//	  Wind:    <50 mph minor | <74 mph moderate | <96 mph severe | ≥96 mph extreme
//	  Tornado: EF0–1 minor | EF2 moderate | EF3–4 severe | EF5 extreme
//
// # ID Generation
//
// Event IDs are deterministic SHA-256 hashes of type|state|lat|lon|time. This
// enables idempotent upserts downstream (ON CONFLICT DO NOTHING) and replay
// safety without distributed coordination. See [generateID].
package domain
