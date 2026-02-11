// Command validate performs end-to-end data integrity checks across all mock
// data sources in the storm data pipeline: source CSVs, collector CSVs, ETL
// JSON, and API JSON. It verifies row counts, field presence, transformation
// correctness, and cross-source consistency.
//
// Usage:
//
//	go run ./cmd/validate \
//	  -source-dir ../storm-data-system/mock-server/data \
//	  -collector-dir ../storm-data-collector/data/mock \
//	  -etl-json data/mock/storm_reports_240426_combined.json \
//	  -api-json ../storm-data-api/data/mock/storm_reports_240426_transformed.json
package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/couchcryptid/storm-data-etl/internal/domain"
	"github.com/jonboulle/clockwork"
)

var baseDate = time.Date(2024, time.April, 26, 0, 0, 0, 0, time.UTC)

// csvSpec maps event types to their CSV file patterns and magnitude columns.
type csvSpec struct {
	sourceFile    string // filename in mock-server/data/
	collectorFile string // filename in collector/data/mock/
	eventType     string
	magCol        string
}

var specs = []csvSpec{
	{sourceFile: "240426_rpts_hail.csv", collectorFile: "240426_rpts_hail.csv", eventType: "hail", magCol: "Size"},
	{sourceFile: "240426_rpts_torn.csv", collectorFile: "240426_rpts_torn.csv", eventType: "tornado", magCol: "F_Scale"},
	{sourceFile: "240426_rpts_wind.csv", collectorFile: "240426_rpts_wind.csv", eventType: "wind", magCol: "Speed"},
}

// phase tracks pass/fail for a validation phase.
type phase struct {
	name   string
	errors []string
}

func (p *phase) errorf(format string, args ...any) {
	p.errors = append(p.errors, fmt.Sprintf(format, args...))
}

func (p *phase) passed() bool { return len(p.errors) == 0 }

func main() {
	sourceDir := flag.String("source-dir", "", "directory containing source NOAA SPC CSV files")
	collectorDir := flag.String("collector-dir", "", "directory containing collector mock CSV files")
	etlJSON := flag.String("etl-json", "", "path to ETL combined JSON fixture")
	apiJSON := flag.String("api-json", "", "path to API transformed JSON fixture")
	flag.Parse()

	if *sourceDir == "" || *collectorDir == "" || *etlJSON == "" || *apiJSON == "" {
		flag.Usage()
		os.Exit(1)
	}

	if code := run(*sourceDir, *collectorDir, *etlJSON, *apiJSON); code != 0 {
		os.Exit(code)
	}
}

func run(sourceDir, collectorDir, etlJSONPath, apiJSONPath string) int {
	// Set a fixed clock matching genmock for ID reproducibility.
	domain.SetClock(clockwork.NewFakeClockAt(
		time.Date(2024, time.April, 27, 6, 0, 0, 0, time.UTC),
	))
	defer domain.SetClock(nil)

	// ── Load all data sources ──
	fmt.Println("=== Storm Data Integrity Validation ===")
	fmt.Println()

	sourceSets, err := loadAllCSVs(sourceDir, func(s csvSpec) string { return s.sourceFile })
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: load source CSVs: %v\n", err)
		return 1
	}

	collectorSets, err := loadAllCSVs(collectorDir, func(s csvSpec) string { return s.collectorFile })
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: load collector CSVs: %v\n", err)
		return 1
	}

	etlRecords, err := loadJSON[domain.RawCSVRecord](etlJSONPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: load ETL JSON: %v\n", err)
		return 1
	}

	apiEvents, err := loadJSON[domain.StormEvent](apiJSONPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: load API JSON: %v\n", err)
		return 1
	}

	// ── Run validation phases ──
	phases := []*phase{
		validateSourceParity(sourceSets, collectorSets),
		validateETLIntegrity(etlRecords, sourceSets),
		validateAPITransformation(apiEvents, etlRecords),
		validateSchemaAlignment(apiEvents),
	}

	// ── Report results ──
	fmt.Println()
	allPassed := true
	for _, p := range phases {
		status := "\033[32mPASS\033[0m"
		if !p.passed() {
			status = fmt.Sprintf("\033[31mFAIL (%d errors)\033[0m", len(p.errors))
			allPassed = false
		}
		fmt.Printf("  %-42s %s\n", p.name, status)
	}

	fmt.Println()
	fmt.Printf("Records: %d source CSV, %d collector CSV, %d ETL JSON, %d API JSON\n",
		countRows(sourceSets), countRows(collectorSets), len(etlRecords), len(apiEvents))

	// Print detailed errors.
	for _, p := range phases {
		if p.passed() {
			continue
		}
		fmt.Printf("\n--- %s ---\n", p.name)
		for i, e := range p.errors {
			fmt.Printf("  [%d] %s\n", i+1, e)
		}
	}

	if allPassed {
		fmt.Println("\nAll validations passed.")
		return 0
	}
	fmt.Println("\nValidation FAILED.")
	return 1
}

// ── Data loading ──

// csvRow is a parsed CSV row with field values keyed by header name.
type csvRow struct {
	lineNum int
	fields  map[string]string
}

// loadAllCSVs loads CSVs for all three event types from a directory.
func loadAllCSVs(dir string, fileNameFn func(csvSpec) string) (map[string][]csvRow, error) {
	result := make(map[string][]csvRow)
	for _, s := range specs {
		path := filepath.Join(dir, fileNameFn(s))
		rows, err := loadCSV(path)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", s.eventType, err)
		}
		result[s.eventType] = rows
	}
	return result, nil
}

func loadCSV(path string) ([]csvRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	all, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(all) < 2 {
		return nil, fmt.Errorf("no data rows in %s", path)
	}

	header := all[0]
	var rows []csvRow
	for i, row := range all[1:] {
		fields := make(map[string]string, len(header))
		for j, h := range header {
			if j < len(row) {
				fields[h] = strings.TrimSpace(row[j])
			}
		}
		rows = append(rows, csvRow{lineNum: i + 2, fields: fields})
	}
	return rows, nil
}

func loadJSON[T any](path string) ([]T, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var items []T
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, err
	}
	return items, nil
}

func countRows(sets map[string][]csvRow) int {
	n := 0
	for _, rows := range sets {
		n += len(rows)
	}
	return n
}

// ── Phase 1: Source Parity ──
// Validates that collector CSVs are identical to the source-of-truth CSVs.

func validateSourceParity(source, collector map[string][]csvRow) *phase {
	p := &phase{name: "Phase 1: Source Parity (CSV files)"}

	for _, s := range specs {
		srcRows := source[s.eventType]
		colRows := collector[s.eventType]

		if len(srcRows) != len(colRows) {
			p.errorf("%s: source has %d rows, collector has %d", s.eventType, len(srcRows), len(colRows))
			continue
		}

		for i := range srcRows {
			for key, srcVal := range srcRows[i].fields {
				colVal, ok := colRows[i].fields[key]
				if !ok {
					p.errorf("%s line %d: collector missing column %q", s.eventType, srcRows[i].lineNum, key)
				} else if srcVal != colVal {
					p.errorf("%s line %d: column %q: source=%q, collector=%q", s.eventType, srcRows[i].lineNum, key, srcVal, colVal)
				}
			}
		}
	}
	return p
}

// ── Phase 2: ETL Integrity ──
// Validates ETL JSON records against source CSVs.

func validateETLIntegrity(etl []domain.RawCSVRecord, source map[string][]csvRow) *phase {
	p := &phase{name: "Phase 2: ETL Integrity (JSON vs CSV)"}

	checkETLCounts(p, etl, source)
	checkETLTypes(p, etl)
	checkETLCrossRef(p, etl, source)
	checkETLMagnitudeColumns(p, etl)

	return p
}

func checkETLCounts(p *phase, etl []domain.RawCSVRecord, source map[string][]csvRow) {
	expectedTotal := countRows(source)
	if len(etl) != expectedTotal {
		p.errorf("total count: expected %d, got %d", expectedTotal, len(etl))
	}

	typeCounts := map[string]int{}
	for i := range etl {
		typeCounts[etl[i].EventType]++
	}
	for _, s := range specs {
		expected := len(source[s.eventType])
		actual := typeCounts[s.eventType]
		if expected != actual {
			p.errorf("%s count: expected %d, got %d", s.eventType, expected, actual)
		}
	}
}

func checkETLTypes(p *phase, etl []domain.RawCSVRecord) {
	validTypes := map[string]bool{"hail": true, "tornado": true, "wind": true}
	for i := range etl {
		if etl[i].EventType == "" {
			p.errorf("ETL record %d: missing EventType field", i)
		} else if !validTypes[etl[i].EventType] {
			p.errorf("ETL record %d: invalid EventType %q", i, etl[i].EventType)
		}
	}
}

func checkETLCrossRef(p *phase, etl []domain.RawCSVRecord, source map[string][]csvRow) {
	etlIndex := map[string]int{}
	for i := range etl {
		key := etl[i].EventType + "|" + etl[i].State + "|" + etl[i].Lat + "|" + etl[i].Lon + "|" + etl[i].Time
		etlIndex[key]++
	}

	for _, s := range specs {
		for _, row := range source[s.eventType] {
			key := s.eventType + "|" + row.fields["State"] + "|" + row.fields["Lat"] + "|" + row.fields["Lon"] + "|" + row.fields["Time"]
			if etlIndex[key] == 0 {
				p.errorf("%s line %d: CSV row not found in ETL JSON (key=%s)", s.eventType, row.lineNum, key)
			}
		}
	}
}

// checkETLMagnitudeColumns verifies each type only populates its own magnitude column.
func checkETLMagnitudeColumns(p *phase, etl []domain.RawCSVRecord) {
	// Map type → columns that must be empty for that type.
	forbidden := map[string][2]struct{ name, field string }{
		"hail":    {{name: "F_Scale"}, {name: "Speed"}},
		"tornado": {{name: "Size"}, {name: "Speed"}},
		"wind":    {{name: "Size"}, {name: "F_Scale"}},
	}

	getField := func(rec domain.RawCSVRecord, name string) string {
		switch name {
		case "Size":
			return rec.Size
		case "F_Scale":
			return rec.FScale
		case "Speed":
			return rec.Speed
		}
		return ""
	}

	for i := range etl {
		cols, ok := forbidden[etl[i].EventType]
		if !ok {
			continue
		}
		for _, col := range cols {
			if v := getField(etl[i], col.name); v != "" {
				p.errorf("ETL record %d: %s record has %s=%q (should be empty)", i, etl[i].EventType, col.name, v)
			}
		}
	}
}

// ── Phase 3: API Transformation ──
// Validates that API JSON was correctly transformed from ETL records.

func validateAPITransformation(api []domain.StormEvent, etl []domain.RawCSVRecord) *phase {
	p := &phase{name: "Phase 3: API Transformation (enrichment)"}

	// Build API index by ID for cross-referencing. When duplicate IDs exist
	// (same type+state+coords+time), keep the first occurrence — matching the
	// database's ON CONFLICT DO NOTHING upsert behavior.
	apiByID := map[string]*domain.StormEvent{}
	var dupeCount int
	for i := range api {
		if api[i].ID == "" {
			p.errorf("API record %d: missing ID", i)
			continue
		}
		if _, exists := apiByID[api[i].ID]; exists {
			dupeCount++
			continue
		}
		apiByID[api[i].ID] = &api[i]
	}

	if dupeCount > 0 {
		fmt.Printf("  Note: %d duplicate ID(s) found (matching DB upsert first-wins behavior)\n", dupeCount)
	}

	// Track which ETL records we've already seen (by ID) to skip duplicates.
	seenIDs := map[string]bool{}

	// Re-run ETL transformation and compare with API output.
	for i := range etl {
		enriched, err := transformETLRecord(etl[i])
		if err != nil {
			p.errorf("ETL record %d: %v", i, err)
			continue
		}

		// Skip duplicate IDs (second occurrence) — matches DB upsert behavior.
		if seenIDs[enriched.ID] {
			continue
		}
		seenIDs[enriched.ID] = true

		apiEvent, ok := apiByID[enriched.ID]
		if !ok {
			p.errorf("ETL record %d (%s): ID %q not found in API JSON", i, etl[i].EventType, enriched.ID)
			continue
		}

		compareEvents(p, enriched, apiEvent)
	}

	return p
}

// transformETLRecord re-runs the ETL transformation on a raw record.
func transformETLRecord(rec domain.RawCSVRecord) (domain.StormEvent, error) {
	rawJSON, err := json.Marshal(rec)
	if err != nil {
		return domain.StormEvent{}, fmt.Errorf("marshal error: %w", err)
	}
	parsed, err := domain.ParseRawEvent(domain.RawEvent{
		Value:     rawJSON,
		Timestamp: baseDate,
	})
	if err != nil {
		return domain.StormEvent{}, fmt.Errorf("parse error: %w", err)
	}
	enriched := domain.EnrichStormEvent(parsed)
	enriched.Source = "spc"
	return enriched, nil
}

// compareEvents checks that an API event matches the expected enriched event.
func compareEvents(p *phase, enriched domain.StormEvent, api *domain.StormEvent) {
	id := enriched.ID

	if api.EventType == "" {
		p.errorf("ID %s: type field is EMPTY in API JSON", id)
	} else if api.EventType != enriched.EventType {
		p.errorf("ID %s: type mismatch: expected %q, got %q", id, enriched.EventType, api.EventType)
	}

	if !floatEq(api.Measurement.Magnitude, enriched.Measurement.Magnitude) {
		p.errorf("ID %s: magnitude: expected %g, got %g", id, enriched.Measurement.Magnitude, api.Measurement.Magnitude)
	}
	if api.Measurement.Unit != enriched.Measurement.Unit {
		p.errorf("ID %s: unit: expected %q, got %q", id, enriched.Measurement.Unit, api.Measurement.Unit)
	}
	if !ptrStrEq(api.Measurement.Severity, enriched.Measurement.Severity) {
		p.errorf("ID %s: severity: expected %s, got %s", id, ptrStr(enriched.Measurement.Severity), ptrStr(api.Measurement.Severity))
	}

	if !api.BeginTime.Equal(enriched.BeginTime) {
		p.errorf("ID %s: begin_time: expected %s, got %s", id, enriched.BeginTime.Format(time.RFC3339), api.BeginTime.Format(time.RFC3339))
	}
	if api.SourceOffice != enriched.SourceOffice {
		p.errorf("ID %s: source_office: expected %q, got %q", id, enriched.SourceOffice, api.SourceOffice)
	}

	if api.Location.Name != enriched.Location.Name {
		p.errorf("ID %s: location.name: expected %q, got %q", id, enriched.Location.Name, api.Location.Name)
	}
	if !ptrFloatEq(api.Location.Distance, enriched.Location.Distance) {
		p.errorf("ID %s: location.distance mismatch", id)
	}
	if !ptrStrEq(api.Location.Direction, enriched.Location.Direction) {
		p.errorf("ID %s: location.direction mismatch", id)
	}

	if !api.TimeBucket.Equal(enriched.TimeBucket) {
		p.errorf("ID %s: time_bucket: expected %s, got %s", id, enriched.TimeBucket.Format(time.RFC3339), api.TimeBucket.Format(time.RFC3339))
	}
}

// ── Phase 4: Schema Alignment ──
// Validates that API JSON field values match GraphQL schema constraints.

func validateSchemaAlignment(api []domain.StormEvent) *phase {
	p := &phase{name: "Phase 4: Schema Alignment (GraphQL)"}
	for i := range api {
		checkSchemaRecord(p, i, &api[i])
	}
	return p
}

var (
	schemaTypes      = map[string]bool{"hail": true, "tornado": true, "wind": true}
	schemaUnits      = map[string]bool{"in": true, "mph": true, "f_scale": true}
	schemaSeverities = map[string]bool{"minor": true, "moderate": true, "severe": true, "extreme": true}
)

func checkSchemaRecord(p *phase, i int, e *domain.StormEvent) {
	pf := func(format string, args ...any) {
		p.errorf("record %d (ID %s): "+format, append([]any{i, e.ID}, args...)...)
	}

	checkSchemaEnums(pf, e)
	checkSchemaRequiredFields(pf, e)
}

func checkSchemaEnums(pf func(string, ...any), e *domain.StormEvent) {
	if e.EventType == "" {
		pf("eventType is empty (schema requires String!)")
	} else if !schemaTypes[e.EventType] {
		pf("eventType %q not in enum {hail, tornado, wind}", e.EventType)
	}

	if e.ID == "" {
		pf("id is empty")
	} else if !strings.HasPrefix(e.ID, e.EventType+"-") {
		pf("id %q doesn't start with type prefix %q-", e.ID, e.EventType)
	}

	if !schemaUnits[e.Measurement.Unit] {
		pf("unit %q not in {in, mph, f_scale}", e.Measurement.Unit)
	}
	if e.Measurement.Severity != nil && !schemaSeverities[*e.Measurement.Severity] {
		pf("severity %q not in {minor, moderate, severe, extreme}", *e.Measurement.Severity)
	}
	if e.Measurement.Magnitude > 0 && e.Measurement.Severity == nil {
		pf("magnitude %g > 0 but severity is nil", e.Measurement.Magnitude)
	}
	if e.Measurement.Magnitude == 0 && e.Measurement.Severity != nil {
		pf("magnitude is 0 but severity is %q", *e.Measurement.Severity)
	}
}

func checkSchemaRequiredFields(pf func(string, ...any), e *domain.StormEvent) {
	if e.Geo.Lat == 0 && e.Geo.Lon == 0 {
		pf("geo coordinates are both zero")
	}
	if e.Location.State == "" {
		pf("location.state is empty")
	} else if len(e.Location.State) != 2 {
		pf("location.state %q is not 2 characters", e.Location.State)
	}
	if e.Location.Name == "" {
		pf("location.name is empty")
	}
	if e.BeginTime.IsZero() {
		pf("begin_time is zero")
	}
	if e.TimeBucket.IsZero() {
		pf("time_bucket is zero")
	}
	if e.Source != "spc" {
		pf("source is %q (expected \"spc\")", e.Source)
	}
	if e.ProcessedAt.IsZero() {
		pf("processed_at is zero")
	}
}

// ── Helpers ──

func floatEq(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

func ptrStrEq(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func ptrFloatEq(a, b *float64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return floatEq(*a, *b)
}

func ptrStr(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
