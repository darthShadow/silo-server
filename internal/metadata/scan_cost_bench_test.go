package metadata_test

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Silo-Server/silo-server/internal/catalog"
	"github.com/Silo-Server/silo-server/internal/libraryingest"
	"github.com/Silo-Server/silo-server/internal/models"
)

// BenchmarkScanCost measures only IngestFolder on a local synthetic tree with
// a warm page cache. series/first includes the fixed 11 s settle wait in
// internal/libraryingest/executor.go:89,297-317. Allocations cover only the Go
// process, including its ingest goroutines; PostgreSQL and ffprobe are separate.
// First-mode write counts vary with drainer timing (one series/first run inserted
// and deleted one more series_root_match_queue row), so compare first-mode numbers
// with benchstat over -count of 6 or more.
func BenchmarkScanCost(b *testing.B) {
	for _, kind := range []string{"movies", "series"} {
		b.Run(kind, func(b *testing.B) {
			tree := scanCostBuildTree(b, kind)
			for _, mode := range []string{"first", "noop"} {
				b.Run(mode, func(b *testing.B) {
					probe := scanCostFakeProbe(b)
					scanCostBenchmark(b, tree, probe, mode == "noop")
				})
			}
		})
	}
}

func scanCostBenchmark(b *testing.B, tree *scanCostTree, probe scanCostProbe, noop bool) {
	b.Helper()
	b.StopTimer()
	writes := make(map[string]scanCostTuples)
	var statements int64
	var allocations uint64
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		measurement, mallocs := scanCostBenchmarkIteration(b, tree, probe, noop)
		allocations += mallocs
		for _, count := range measurement.tracer.statements {
			statements += count
		}
		for table, delta := range measurement.writes {
			total := writes[table]
			total.ins += delta.ins
			total.upd += delta.upd
			total.del += delta.del
			writes[table] = total
		}
	}
	scanCostBenchmarkMetrics(b, tree.wantFiles, statements, allocations, writes)
}

func scanCostBenchmarkIteration(b *testing.B, tree *scanCostTree, probe scanCostProbe, noop bool) (scanCostMeasurement, uint64) {
	b.Helper()
	config, cleanup := scanCostNewClone(b)
	defer cleanup()
	provider := &scanCostProvider{titles: tree.titles}
	var folder *models.MediaFolder
	setup := func(ctx context.Context, pool *pgxpool.Pool) error {
		folderType := "movies"
		if tree.kind == "series" {
			folderType = "tv"
		}
		var err error
		folder, err = catalog.NewFolderRepository(pool).Create(ctx, catalog.CreateFolderInput{
			Name: "Scan cost " + tree.kind, Type: folderType, Paths: tree.roots,
		})
		if err != nil || !noop {
			return err
		}
		harness := scanCostNewHarness(pool, probe, provider)
		result, err := harness.executor.IngestFolder(ctx, folder)
		scanCostCheckOutcome(b, scanCostPass{result: result, err: err}, false, tree.wantFiles)
		return err
	}
	var before, after runtime.MemStats
	var result *libraryingest.Result
	measurement, err := scanCostMeasure(b.Context(), config, setup, func(ctx context.Context, pool *pgxpool.Pool) error {
		harness := scanCostNewHarness(pool, probe, provider)
		// Keep allocation sampling and harness wiring outside the timer.
		runtime.ReadMemStats(&before)
		b.StartTimer()
		var err error
		result, err = harness.executor.IngestFolder(ctx, folder)
		b.StopTimer()
		runtime.ReadMemStats(&after)
		return err
	})
	if err != nil {
		b.Fatal(err)
	}
	scanCostCheckOutcome(b, scanCostPass{result: result}, noop, tree.wantFiles)
	if b.Failed() {
		b.FailNow()
	}
	return measurement, after.Mallocs - before.Mallocs
}

func scanCostBenchmarkMetrics(b *testing.B, files int, statements int64, allocations uint64, writes map[string]scanCostTuples) {
	b.Helper()
	var tuples int64
	names := make([]string, 0, len(writes))
	for table, delta := range writes {
		names = append(names, table)
		tuples += delta.ins + delta.upd + delta.del
	}
	slices.Sort(names)
	entries := make([]string, 0, len(names))
	for _, table := range names {
		delta := writes[table]
		entries = append(entries, fmt.Sprintf("%s={ins:%g upd:%g del:%g}", table,
			float64(delta.ins)/float64(b.N), float64(delta.upd)/float64(b.N), float64(delta.del)/float64(b.N)))
	}
	// Go truncates benchmark logs after ten lines; keep the sorted map on one.
	b.Logf("scan-cost per-table tuples/op: %s", strings.Join(entries, "; "))
	perThousand := 1000 / (float64(files) * float64(b.N))
	b.ReportMetric(float64(files), "files/op")
	b.ReportMetric(float64(statements)*perThousand, "stmts/kfile")
	b.ReportMetric(float64(tuples)*perThousand, "tuples/kfile")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())*perThousand, "ns/kfile")
	b.ReportMetric(float64(allocations)*perThousand, "allocs/kfile")
}
