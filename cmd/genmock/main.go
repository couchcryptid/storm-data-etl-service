// Command genmock reads NOAA SPC CSV files and generates mock data fixtures
// for both the ETL and API test suites. It uses the actual ETL domain package
// to ensure the transformed output matches real pipeline behavior.
//
// Usage:
//
//	go run ./cmd/genmock \
//	  -csv-dir ../storm-data-system/mock-server/data \
//	  -etl-out data/mock/storm_reports_240426_combined.json \
//	  -api-out ../storm-data-api/data/mock/storm_reports_240426_transformed.json
package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/couchcryptid/storm-data-etl/internal/domain"
	"github.com/jonboulle/clockwork"
)

var baseDate = time.Date(2024, time.April, 26, 0, 0, 0, 0, time.UTC)

type csvDef struct {
	file      string
	eventType string
	magCol    string // column name for magnitude (Size, F_Scale, Speed)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	csvDir := flag.String("csv-dir", "", "directory containing NOAA SPC CSV files")
	etlOut := flag.String("etl-out", "", "output path for ETL raw JSON fixture")
	apiOut := flag.String("api-out", "", "output path for API transformed JSON fixture")
	flag.Parse()

	if *csvDir == "" || *etlOut == "" || *apiOut == "" {
		flag.Usage()
		return fmt.Errorf("missing required flags: -csv-dir, -etl-out, -api-out")
	}

	defs := []csvDef{
		{file: "240426_rpts_hail.csv", eventType: "hail", magCol: "Size"},
		{file: "240426_rpts_torn.csv", eventType: "tornado", magCol: "F_Scale"},
		{file: "240426_rpts_wind.csv", eventType: "wind", magCol: "Speed"},
	}

	// Set a fixed clock for reproducible ProcessedAt timestamps.
	domain.SetClock(clockwork.NewFakeClockAt(
		time.Date(2024, time.April, 27, 6, 0, 0, 0, time.UTC),
	))
	defer domain.SetClock(nil)

	var rawRecords []domain.RawCSVRecord //nolint:prealloc // size depends on CSV file contents
	var transformed []domain.StormEvent  //nolint:prealloc // size depends on CSV file contents

	for _, d := range defs {
		path := filepath.Join(*csvDir, d.file)
		recs, events, err := processCSV(path, d.eventType, d.magCol)
		if err != nil {
			return fmt.Errorf("processing %s: %w", d.file, err)
		}
		rawRecords = append(rawRecords, recs...)
		transformed = append(transformed, events...)
		log.Printf("%s: %d records", d.eventType, len(recs))
	}

	log.Printf("total: %d records", len(rawRecords))

	if err := writeJSON(*etlOut, rawRecords); err != nil {
		return fmt.Errorf("writing ETL fixture: %w", err)
	}
	log.Printf("wrote ETL fixture: %s", *etlOut)

	if err := writeJSON(*apiOut, transformed); err != nil {
		return fmt.Errorf("writing API fixture: %w", err)
	}
	log.Printf("wrote API fixture: %s", *apiOut)

	printStats(transformed)
	return nil
}

func processCSV(path, eventType, magCol string) ([]domain.RawCSVRecord, []domain.StormEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("read csv: %w", err)
	}

	if len(rows) < 2 {
		return nil, nil, fmt.Errorf("no data rows")
	}

	header := rows[0]
	colIdx := map[string]int{}
	for i, h := range header {
		colIdx[h] = i
	}

	var recs []domain.RawCSVRecord
	var events []domain.StormEvent

	for _, row := range rows[1:] {
		if len(row) < len(header) {
			continue
		}

		rec := domain.RawCSVRecord{
			Time:      get(row, colIdx, "Time"),
			Location:  get(row, colIdx, "Location"),
			County:    get(row, colIdx, "County"),
			State:     get(row, colIdx, "State"),
			Lat:       get(row, colIdx, "Lat"),
			Lon:       get(row, colIdx, "Lon"),
			Comments:  get(row, colIdx, "Comments"),
			EventType: eventType,
		}

		// Set the type-specific magnitude field.
		mag := get(row, colIdx, magCol)
		switch eventType {
		case "hail":
			rec.Size = mag
		case "tornado":
			rec.FScale = mag
		case "wind":
			rec.Speed = mag
		}

		recs = append(recs, rec)

		// Run the actual ETL transformation.
		rawJSON, err := json.Marshal(rec)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal record: %w", err)
		}

		rawEvent := domain.RawEvent{
			Value:     rawJSON,
			Timestamp: baseDate,
		}

		parsed, err := domain.ParseRawEvent(rawEvent)
		if err != nil {
			return nil, nil, fmt.Errorf("parse raw event: %w", err)
		}

		enriched := domain.EnrichStormEvent(parsed)
		enriched.Source = "spc"
		events = append(events, enriched)
	}

	return recs, events, nil
}

func get(row []string, idx map[string]int, col string) string {
	i, ok := idx[col]
	if !ok || i >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[i])
}

func writeJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o600)
}

// statsResult holds aggregated counts for printStats reporting.
type statsResult struct {
	typeCounts     map[string]int
	severityCounts map[string]int
	stateCounts    map[string]int
	countyCounts   map[string]int
	withSeverity   int
	mag175plus     int
}

func collectStats(events []domain.StormEvent) statsResult {
	s := statsResult{
		typeCounts:     map[string]int{},
		severityCounts: map[string]int{},
		stateCounts:    map[string]int{},
		countyCounts:   map[string]int{},
	}
	for i := range events {
		e := &events[i]
		s.typeCounts[e.EventType]++
		s.stateCounts[e.Location.State]++
		s.countyCounts[e.Location.State+":"+e.Location.County]++

		if e.Measurement.Severity != nil {
			s.severityCounts[*e.Measurement.Severity]++
			s.withSeverity++
		}
		if e.Measurement.Magnitude >= 1.75 {
			s.mag175plus++
		}
	}
	return s
}

type stateCount struct {
	state string
	count int
}

func printStats(events []domain.StormEvent) {
	stats := collectStats(events)

	fmt.Println("\n=== Stats for updating test assertions ===")
	fmt.Printf("Total: %d\n", len(events))
	fmt.Printf("By type: hail=%d, tornado=%d, wind=%d\n",
		stats.typeCounts["hail"], stats.typeCounts["tornado"], stats.typeCounts["wind"])
	fmt.Printf("With severity: %d\n", stats.withSeverity)
	fmt.Printf("By severity: minor=%d, moderate=%d, severe=%d, extreme=%d\n",
		stats.severityCounts["minor"], stats.severityCounts["moderate"],
		stats.severityCounts["severe"], stats.severityCounts["extreme"])
	fmt.Printf("Magnitude >= 1.75: %d\n", stats.mag175plus)

	printStateBreakdown(stats)
	printFilterCombos(events)
	printGeoFilter(events)
	printHailDetails(events)
}

func printStateBreakdown(stats statsResult) {
	sc := make([]stateCount, 0, len(stats.stateCounts))
	for s, c := range stats.stateCounts {
		sc = append(sc, stateCount{s, c})
	}
	sort.Slice(sc, func(i, j int) bool { return sc[i].count > sc[j].count })
	fmt.Printf("States (%d): ", len(sc))
	for _, s := range sc {
		fmt.Printf("%s=%d ", s.state, s.count)
	}
	fmt.Println()

	// Print county details for states with many reports.
	fmt.Println("\nCounty breakdown for top states:")
	for _, s := range sc[:min(5, len(sc))] {
		fmt.Printf("  %s (%d):", s.state, s.count)
		var counties []stateCount
		for k, c := range stats.countyCounts {
			parts := strings.SplitN(k, ":", 2)
			if parts[0] == s.state {
				counties = append(counties, stateCount{parts[1], c})
			}
		}
		sort.Slice(counties, func(i, j int) bool { return counties[i].count > counties[j].count })
		for _, c := range counties {
			fmt.Printf(" %s=%d", c.state, c.count)
		}
		fmt.Println()
	}
}

func printFilterCombos(events []domain.StormEvent) {
	var tarrantCount int
	var severeHailTX int
	var hailPlusTornado int
	for i := range events {
		e := &events[i]
		if e.Location.County == "Tarrant" {
			tarrantCount++
		}
		if e.EventType == "hail" && e.Location.State == "TX" && e.Measurement.Severity != nil && *e.Measurement.Severity == "severe" {
			severeHailTX++
		}
		if e.EventType == "hail" || e.EventType == "tornado" {
			hailPlusTornado++
		}
	}
	fmt.Printf("\nTarrant County: %d\n", tarrantCount)
	fmt.Printf("Severe hail in TX: %d\n", severeHailTX)
	fmt.Printf("Hail + Tornado: %d\n", hailPlusTornado)
}

func printGeoFilter(events []domain.StormEvent) {
	// Geo filter: reports near Fort Worth (32.75, -97.15) within ~0.75 degrees.
	var nearFW int
	for i := range events {
		e := &events[i]
		latDiff := e.Geo.Lat - 32.75
		lonDiff := e.Geo.Lon - (-97.15)
		if latDiff < 0 {
			latDiff = -latDiff
		}
		if lonDiff < 0 {
			lonDiff = -lonDiff
		}
		if latDiff <= 0.75 && lonDiff <= 0.75 {
			nearFW++
		}
	}
	fmt.Printf("Near Fort Worth (±0.75°): %d\n", nearFW)
}

func printHailDetails(events []domain.StormEvent) {
	// First hail record info.
	for i := range events {
		if events[i].EventType != "hail" {
			continue
		}
		e := &events[i]
		fmt.Printf("\nFirst hail record:\n")
		fmt.Printf("  ID: %s\n", e.ID)
		fmt.Printf("  Lat: %g, Lon: %g\n", e.Geo.Lat, e.Geo.Lon)
		fmt.Printf("  Magnitude: %g, Unit: %s\n", e.Measurement.Magnitude, e.Measurement.Unit)
		if e.Measurement.Severity != nil {
			fmt.Printf("  Severity: %s\n", *e.Measurement.Severity)
		}
		fmt.Printf("  Location: %s (name=%s, state=%s, county=%s)\n",
			e.Location.Raw, e.Location.Name, e.Location.State, e.Location.County)
		fmt.Printf("  SourceOffice: %s\n", e.SourceOffice)
		fmt.Printf("  BeginTime: %s\n", e.BeginTime.Format(time.RFC3339))
		fmt.Printf("  TimeBucket: %s\n", e.TimeBucket.Format(time.RFC3339))
		break
	}

	// Max hail magnitude.
	var maxHailMag float64
	for i := range events {
		if events[i].EventType == "hail" && events[i].Measurement.Magnitude > maxHailMag {
			maxHailMag = events[i].Measurement.Magnitude
		}
	}
	fmt.Printf("\nMax hail magnitude: %g in\n", maxHailMag)
}
