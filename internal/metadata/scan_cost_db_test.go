package metadata_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Silo-Server/silo-server/internal/catalog"
	"github.com/Silo-Server/silo-server/internal/database"
	"github.com/Silo-Server/silo-server/internal/libraryingest"
	"github.com/Silo-Server/silo-server/internal/metadata"
	"github.com/Silo-Server/silo-server/internal/models"
	"github.com/Silo-Server/silo-server/internal/scanner"
	"github.com/Silo-Server/silo-server/migrations"
)

const (
	scanCostMaxConns       = 20
	scanCostBarrierTimeout = 30 * time.Second
	scanCostBarrierPoll    = 10 * time.Millisecond
	scanCostFolderPathsSQL = "SELECT media_folder_id, path FROM media_folder_paths WHERE media_folder_id = ANY($1) ORDER BY id ASC"
)

var scanCostTemplate struct {
	once   sync.Once
	config *pgxpool.Config
	name   string
	err    error
	next   atomic.Uint64
}

func TestMain(m *testing.M) {
	code := m.Run()
	if scanCostTemplate.name != "" {
		ctx, cancel := context.WithTimeout(context.Background(), scanCostBarrierTimeout)
		if err := scanCostDropDatabase(ctx, scanCostTemplate.config, scanCostTemplate.name); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "drop scan-cost template: %v\n", err)
		}
		cancel()
	}
	os.Exit(code)
}

func scanCostOpenPool(ctx context.Context, config *pgxpool.Config, tracer *scanCostTracer) (*pgxpool.Pool, error) {
	config = config.Copy()
	config.ConnConfig.RuntimeParams["jit"] = "off"
	config.ConnConfig.Tracer = nil
	config.AfterConnect = nil
	config.MinConns = 0
	config.MinIdleConns = 0
	if tracer != nil {
		config.MaxConns = scanCostMaxConns
		config.ConnConfig.Tracer = tracer
	}
	return pgxpool.NewWithConfig(ctx, config)
}

func scanCostInitTemplate(ctx context.Context, dsn string) error {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse scan-cost database config: %w", err)
	}
	scanCostTemplate.config = config
	admin, err := scanCostOpenPool(ctx, config, nil)
	if err != nil {
		return fmt.Errorf("open scan-cost admin pool: %w", err)
	}
	defer admin.Close()
	name := fmt.Sprintf("silo_scancost_%d_%d_tpl", os.Getpid(), time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		return fmt.Errorf("create scan-cost template: %w", err)
	}
	scanCostTemplate.name = name
	templateConfig := config.Copy()
	templateConfig.ConnConfig.Database = name
	pool, err := scanCostOpenPool(ctx, templateConfig, nil)
	if err != nil {
		return fmt.Errorf("open scan-cost template pool: %w", err)
	}
	err = database.RunMigrations(ctx, pool, migrations.FS, "sql")
	pool.Close()
	if err != nil {
		return fmt.Errorf("migrate scan-cost template: %w", err)
	}
	conn, err := admin.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire scan-cost admin connection: %w", err)
	}
	defer conn.Release()
	return scanCostBarrier(ctx, conn.Conn(), name)
}

func scanCostClone(t testing.TB) *pgxpool.Config {
	t.Helper()
	config, cleanup := scanCostNewClone(t)
	t.Cleanup(cleanup)
	return config
}

// scanCostNewClone lets benchmarks drop a clone before the next iteration.
func scanCostNewClone(t testing.TB) (*pgxpool.Config, func()) {
	t.Helper()
	dsn := os.Getenv("SILO_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("SILO_TEST_DATABASE_URL is not set")
	}
	ctx := t.Context()
	scanCostTemplate.once.Do(func() {
		scanCostTemplate.err = scanCostInitTemplate(ctx, dsn)
	})
	if scanCostTemplate.err != nil {
		t.Fatal(scanCostTemplate.err)
	}
	name := fmt.Sprintf("%s_%d", scanCostTemplate.name, scanCostTemplate.next.Add(1))
	admin, err := scanCostOpenPool(ctx, scanCostTemplate.config, nil)
	if err != nil {
		t.Fatalf("open scan-cost clone admin pool: %v", err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+
		" TEMPLATE "+pgx.Identifier{scanCostTemplate.name}.Sanitize()); err != nil {
		t.Fatalf("clone scan-cost database: %v", err)
	}
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), scanCostBarrierTimeout)
		defer cancel()
		if err := scanCostDropDatabase(ctx, scanCostTemplate.config, name); err != nil {
			t.Errorf("drop scan-cost clone: %v", err)
		}
	}
	var backends int
	if err := admin.QueryRow(ctx, scanCostBackendsSQL, name).Scan(&backends); err != nil {
		cleanup()
		t.Fatalf("check fresh scan-cost clone sessions: %v", err)
	}
	if backends != 0 {
		cleanup()
		t.Fatalf("fresh scan-cost clone has %d client backends, want 0", backends)
	}
	config := scanCostTemplate.config.Copy()
	config.ConnConfig.Database = name
	return config, cleanup
}

func scanCostDropDatabase(ctx context.Context, config *pgxpool.Config, name string) error {
	admin, err := scanCostOpenPool(ctx, config, nil)
	if err != nil {
		return err
	}
	defer admin.Close()
	_, err = admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
	return err
}

const scanCostBackendsSQL = `SELECT count(*) FROM pg_stat_activity
	WHERE datname = $1 AND backend_type = 'client backend' AND pid <> pg_backend_pid()`

func scanCostBarrier(ctx context.Context, conn *pgx.Conn, name string) error {
	ctx, cancel := context.WithTimeout(ctx, scanCostBarrierTimeout)
	defer cancel()
	ticker := time.NewTicker(scanCostBarrierPoll)
	defer ticker.Stop()
	for {
		var backends int
		if err := conn.QueryRow(ctx, scanCostBackendsSQL, name).Scan(&backends); err != nil {
			return fmt.Errorf("scan-cost backend barrier: %w", err)
		}
		if backends == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("scan-cost backend barrier: %d client backends remain: %w", backends, ctx.Err())
		case <-ticker.C:
		}
	}
}

type scanCostPhaseKey struct{}
type scanCostSQLKey struct{}

func scanCostPhase(ctx context.Context) string {
	phase, _ := ctx.Value(scanCostPhaseKey{}).(string)
	switch phase {
	case "drain", "match", "finalize":
		return phase
	default:
		return "other"
	}
}

func scanCostFolderRead(phase, sql string) bool {
	return phase == "other" && (sql == scanCostFolderPathsSQL ||
		(strings.HasPrefix(sql, "SELECT ") && strings.HasSuffix(sql, " FROM media_folders WHERE id = $1")))
}

type scanCostStatementError struct {
	phase      string
	folderRead bool
	sql        string
	err        error
}

func (e *scanCostStatementError) Error() string {
	return fmt.Sprintf("scan-cost statement phase=%s folder-read=%t SQL=%q: %v", e.phase, e.folderRead, e.sql, e.err)
}

func (e *scanCostStatementError) Unwrap() error { return e.err }

type scanCostTracer struct {
	mu                  sync.Mutex
	statements          map[string]int64
	folderReads         int64
	exemptCancellations int64
	errors              []*scanCostStatementError
}

func (tracer *scanCostTracer) count(ctx context.Context, sql string) {
	phase := scanCostPhase(ctx)
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	tracer.statements[phase]++
	if scanCostFolderRead(phase, sql) {
		tracer.folderReads++
	}
}

func (tracer *scanCostTracer) recordError(ctx context.Context, sql string, err error) {
	if err == nil {
		return
	}
	phase := scanCostPhase(ctx)
	folderRead := scanCostFolderRead(phase, sql)
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	if folderRead && errors.Is(err, context.Canceled) {
		tracer.exemptCancellations++
		return
	}
	tracer.errors = append(tracer.errors, &scanCostStatementError{
		phase: phase, folderRead: folderRead, sql: sql, err: err,
	})
}

func (tracer *scanCostTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tracer.count(ctx, data.SQL)
	return context.WithValue(ctx, scanCostSQLKey{}, data.SQL)
}

func (tracer *scanCostTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	sql, _ := ctx.Value(scanCostSQLKey{}).(string)
	tracer.recordError(ctx, sql, data.Err)
}

func (*scanCostTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	statements := make([]string, len(data.Batch.QueuedQueries))
	for i, query := range data.Batch.QueuedQueries {
		statements[i] = query.SQL
	}
	return context.WithValue(ctx, scanCostSQLKey{}, strings.Join(statements, ";\n"))
}

func (tracer *scanCostTracer) TraceBatchQuery(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchQueryData) {
	tracer.count(ctx, data.SQL)
	tracer.recordError(ctx, data.SQL, data.Err)
}

func (tracer *scanCostTracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	sql, _ := ctx.Value(scanCostSQLKey{}).(string)
	tracer.recordError(ctx, sql, data.Err)
}

func TestScanCostTracerCancellationExemption(t *testing.T) {
	tests := []struct {
		name                    string
		phase                   string
		sql                     string
		err                     error
		wantExemptCancellations int64
		wantStatementErrors     int
	}{
		{
			name:                    "bucket read canceled",
			sql:                     scanCostFolderPathsSQL,
			err:                     fmt.Errorf("query canceled: %w", context.Canceled),
			wantExemptCancellations: 1,
			wantStatementErrors:     0,
		},
		{
			name:                    "non-bucket statement canceled",
			sql:                     "SELECT 1",
			err:                     fmt.Errorf("query canceled: %w", context.Canceled),
			wantExemptCancellations: 0,
			wantStatementErrors:     1,
		},
		{
			name:                    "bucket read fails otherwise",
			sql:                     scanCostFolderPathsSQL,
			err:                     errors.New("query failed"),
			wantExemptCancellations: 0,
			wantStatementErrors:     1,
		},
		{
			name:                    "folder read outside other canceled",
			phase:                   "drain",
			sql:                     scanCostFolderPathsSQL,
			err:                     fmt.Errorf("query canceled: %w", context.Canceled),
			wantExemptCancellations: 0,
			wantStatementErrors:     1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracer := &scanCostTracer{statements: make(map[string]int64)}
			ctx := t.Context()
			if tt.phase != "" {
				ctx = context.WithValue(ctx, scanCostPhaseKey{}, tt.phase)
			}
			ctx = tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: tt.sql})
			tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: tt.err})
			if got := tracer.exemptCancellations; got != tt.wantExemptCancellations {
				t.Errorf("exempt cancellations: observed %d, want %d", got, tt.wantExemptCancellations)
			}
			if got := len(tracer.errors); got != tt.wantStatementErrors {
				t.Errorf("statement errors: observed %d, want %d", got, tt.wantStatementErrors)
			}
		})
	}
}

type scanCostTuples struct {
	ins int64
	upd int64
	del int64
}

func scanCostSample(ctx context.Context, conn *pgx.Conn) (map[string]scanCostTuples, error) {
	rows, err := conn.Query(ctx, `SELECT relname, n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sample := make(map[string]scanCostTuples)
	for rows.Next() {
		var table string
		var tuples scanCostTuples
		if err := rows.Scan(&table, &tuples.ins, &tuples.upd, &tuples.del); err != nil {
			return nil, err
		}
		sample[table] = tuples
	}
	return sample, rows.Err()
}

func scanCostSampler(ctx context.Context, config *pgxpool.Config) (*pgx.Conn, error) {
	connConfig := config.ConnConfig.Copy()
	connConfig.Tracer = nil
	connConfig.RuntimeParams["jit"] = "off"
	connConfig.RuntimeParams["stats_fetch_consistency"] = "none"
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("connect scan-cost sampler: %w", err)
	}
	var trackCounts string
	err = conn.QueryRow(ctx, "SELECT current_setting('track_counts')").Scan(&trackCounts)
	if err == nil && trackCounts != "on" {
		err = fmt.Errorf("scan-cost track_counts = %q, want on", trackCounts)
	}
	if err != nil {
		return nil, errors.Join(err, conn.Close(context.WithoutCancel(ctx)))
	}
	return conn, nil
}

type scanCostOperation func(context.Context, *pgxpool.Pool) error

type scanCostMeasurement struct {
	writes map[string]scanCostTuples
	tracer *scanCostTracer
	wall   time.Duration
}

func scanCostSetup(ctx context.Context, config *pgxpool.Config, setup scanCostOperation) error {
	if setup == nil {
		return nil
	}
	pool, err := scanCostOpenPool(ctx, config, nil)
	if err != nil {
		return err
	}
	defer pool.Close()
	return setup(ctx, pool)
}

func scanCostWarmPool(ctx context.Context, pool *pgxpool.Pool) error {
	conns := make([]*pgxpool.Conn, 0, scanCostMaxConns)
	defer func() {
		for _, conn := range conns {
			conn.Release()
		}
	}()
	for range scanCostMaxConns {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("warm scan-cost measured pool: %w", err)
		}
		conns = append(conns, conn)
	}
	return nil
}

func scanCostMeasure(ctx context.Context, config *pgxpool.Config, setup, operation scanCostOperation) (result scanCostMeasurement, err error) {
	if err := scanCostSetup(ctx, config, setup); err != nil {
		return result, fmt.Errorf("scan-cost setup: %w", err)
	}
	sampler, err := scanCostSampler(ctx, config)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, sampler.Close(context.WithoutCancel(ctx))) }()
	if err := scanCostBarrier(ctx, sampler, config.ConnConfig.Database); err != nil {
		return result, err
	}
	baseline, err := scanCostSample(ctx, sampler)
	if err != nil {
		return result, fmt.Errorf("scan-cost baseline sample: %w", err)
	}
	result.tracer = &scanCostTracer{statements: make(map[string]int64)}
	pool, err := scanCostOpenPool(ctx, config, result.tracer)
	if err != nil {
		return result, err
	}
	defer pool.Close()
	if err := scanCostWarmPool(ctx, pool); err != nil {
		return result, err
	}
	start := time.Now()
	operationErr := operation(ctx, pool)
	result.wall = time.Since(start)
	pool.Close()
	barrierErr := scanCostBarrier(ctx, sampler, config.ConnConfig.Database)
	err = errors.Join(operationErr, barrierErr)
	for _, statementErr := range result.tracer.errors {
		err = errors.Join(err, statementErr)
	}
	if barrierErr != nil {
		return result, err
	}
	final, sampleErr := scanCostSample(ctx, sampler)
	if sampleErr != nil {
		return result, errors.Join(err, fmt.Errorf("scan-cost final sample: %w", sampleErr))
	}
	result.writes = make(map[string]scanCostTuples, len(final)+len(baseline))
	for table, counts := range final {
		result.writes[table] = counts
	}
	for table, counts := range baseline {
		last := result.writes[table]
		result.writes[table] = scanCostTuples{ins: last.ins - counts.ins, upd: last.upd - counts.upd, del: last.del - counts.del}
	}
	return result, err
}

func scanCostCheckWrites(t testing.TB, got, want map[string]scanCostTuples) {
	t.Helper()
	tables := make(map[string]struct{}, len(got)+len(want))
	for table := range got {
		tables[table] = struct{}{}
	}
	for table := range want {
		tables[table] = struct{}{}
	}
	names := make([]string, 0, len(tables))
	for table := range tables {
		names = append(names, table)
	}
	slices.Sort(names)
	mismatch := false
	for _, table := range names {
		if got[table] != want[table] {
			t.Errorf("scan-cost table %s: want %+v, observed %+v", table, want[table], got[table])
			mismatch = true
		}
	}
	if mismatch {
		t.Error("update the want map and label each change legitimate or debt")
	}
}

func scanCostSeedFolder(ctx context.Context, pool *pgxpool.Pool, kind string) (int64, error) {
	var id int64
	err := pool.QueryRow(ctx, `INSERT INTO media_folders (type, name) VALUES ($1, 'Scan cost control') RETURNING id`, kind).Scan(&id)
	return id, err
}

func scanCostControl(t *testing.T, config *pgxpool.Config, setup, operation scanCostOperation, want map[string]scanCostTuples) {
	t.Helper()
	result, err := scanCostMeasure(t.Context(), config, setup, operation)
	if err != nil {
		t.Fatalf("scan-cost measurement: %v", err)
	}
	t.Logf("statements=%v folder-reads=%d exempt-cancellations=%d wall=%s",
		result.tracer.statements, result.tracer.folderReads, result.tracer.exemptCancellations, result.wall)
	scanCostCheckWrites(t, result.writes, want)
}

// TestScanCostCounterSeesNestedWritesDB checks the counter against writes hidden
// by SELECT and UPDATE command tags. Setup always closes before the baseline.
func TestScanCostCounterSeesNestedWritesDB(t *testing.T) {
	config := scanCostClone(t)
	t.Run("setup_does_not_leak", func(t *testing.T) {
		scanCostControl(t, config, func(ctx context.Context, pool *pgxpool.Pool) error {
			_, err := scanCostSeedFolder(ctx, pool, "movies")
			return err
		}, func(ctx context.Context, pool *pgxpool.Pool) error {
			_, err := pool.Exec(ctx, "SELECT 1")
			return err
		}, nil)
	})
	t.Run("write_under_select", func(t *testing.T) {
		scanCostControlWriteUnderSelect(t, config)
	})
	t.Run("insert_under_update_with_triggers", func(t *testing.T) {
		scanCostControlInsertUnderUpdate(t, config)
	})
	t.Run("swallowed_statement_error", func(t *testing.T) {
		_, err := scanCostMeasure(t.Context(), config, nil, func(ctx context.Context, pool *pgxpool.Pool) error {
			_, _ = pool.Exec(ctx, "SELECT 1 FROM scan_cost_missing_table")
			return nil
		})
		if err == nil {
			t.Fatal("scan-cost helper error = nil, want swallowed statement error SQLSTATE 42P01")
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42P01" {
			t.Fatalf("scan-cost helper error = %v, want *pgconn.PgError with SQLSTATE 42P01", err)
		}
	})
}

func scanCostOwnTag(tag pgconn.CommandTag, err error, want string) error {
	if err != nil {
		return err
	}
	if tag.String() != want {
		return fmt.Errorf("scan-cost own command tag = %q, want %q", tag.String(), want)
	}
	return nil
}

func scanCostControlWriteUnderSelect(t *testing.T, config *pgxpool.Config) {
	t.Helper()
	var fileID int64
	scanCostControl(t, config, func(ctx context.Context, pool *pgxpool.Pool) error {
		folderID, err := scanCostSeedFolder(ctx, pool, "movies")
		if err != nil {
			return err
		}
		if err := pool.QueryRow(ctx, `INSERT INTO media_files (media_folder_id, file_path, base_type, file_size)
			VALUES ($1, '/scan-cost/control/movie.mp4', 'movie', 1) RETURNING id`, folderID).Scan(&fileID); err != nil {
			return err
		}
		_, err = pool.Exec(ctx, `INSERT INTO movie_match_queue (media_file_id, media_folder_id, available_at)
			VALUES ($1, $2, NOW() - INTERVAL '1 minute')`, fileID, folderID)
		return err
	}, func(ctx context.Context, pool *pgxpool.Pool) error {
		// Claim form from movie_match_queue_repo.go, eligible only for the seed.
		tag, err := pool.Exec(ctx, `
			WITH candidates AS (
				SELECT q.media_file_id, q.available_at, q.last_attempted_at,
					(q.rerun_requested OR q.lease_forced_rerun) AS rerun_requested
				FROM movie_match_queue q
				JOIN media_files mf ON mf.id = q.media_file_id
				JOIN media_folders folders ON folders.id = mf.media_folder_id
				LEFT JOIN media_items mi ON mi.content_id = mf.content_id
				WHERE q.media_file_id = $1 AND q.state = 'pending' AND q.available_at <= NOW()
				ORDER BY q.available_at ASC, q.last_attempted_at ASC NULLS FIRST, q.media_file_id ASC
				LIMIT 1
				FOR UPDATE OF q SKIP LOCKED
			), updated AS (
				UPDATE movie_match_queue q
				SET last_attempted_at = NOW(), attempt_count = q.attempt_count + 1,
					available_at = NOW() + INTERVAL '1 minute', lease_token = 'scan-cost-control',
					lease_forced_rerun = c.rerun_requested, rerun_requested = false, updated_at = NOW()
				FROM candidates c
				WHERE q.media_file_id = c.media_file_id
				RETURNING q.media_file_id
			)
			SELECT c.media_file_id, c.rerun_requested
			FROM candidates c JOIN updated u ON u.media_file_id = c.media_file_id
			ORDER BY c.available_at ASC, c.last_attempted_at ASC NULLS FIRST, c.media_file_id ASC
		`, fileID)
		return scanCostOwnTag(tag, err, "SELECT 1")
	}, map[string]scanCostTuples{"movie_match_queue": {upd: 1}})
}

func scanCostControlInsertUnderUpdate(t *testing.T, config *pgxpool.Config) {
	t.Helper()
	var folderID int64
	scanCostControl(t, config, func(ctx context.Context, pool *pgxpool.Pool) error {
		var err error
		folderID, err = scanCostSeedFolder(ctx, pool, "tv")
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, `INSERT INTO media_items (content_id, type, title)
			VALUES ('scan-cost-series', 'series', 'Scan Cost Series')`); err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, `INSERT INTO episodes (content_id, series_id, season_number, episode_number, title)
			VALUES ('scan-cost-episode', 'scan-cost-series', 1, 1, 'Scan Cost Episode')`); err != nil {
			return err
		}
		_, err = pool.Exec(ctx, `INSERT INTO media_files (media_folder_id, file_path, base_type, file_size, episode_id)
			VALUES ($1, '/scan-cost/control/series/episode.mp4', 'series', 1, 'scan-cost-episode')`, folderID)
		return err
	}, func(ctx context.Context, pool *pgxpool.Pool) error {
		// Copy the scanner's membership restore with an empty file predicate.
		tag, err := pool.Exec(ctx, `
			WITH target_episode AS (
				SELECT DISTINCT mf.episode_id, mf.media_folder_id
				FROM media_files mf JOIN episodes e ON e.content_id = mf.episode_id
				WHERE mf.media_folder_id = $1 AND mf.missing_since IS NULL AND mf.episode_id IS NOT NULL
				  AND NOT EXISTS (
					SELECT 1 FROM episode_libraries el
					WHERE el.episode_id = mf.episode_id AND el.media_folder_id = mf.media_folder_id
				  )
			), candidate AS (
				SELECT target.episode_id, target.media_folder_id, MIN(mf.created_at) AS first_seen_at,
					(array_agg(mf.first_seen_scan_run_id ORDER BY mf.created_at ASC, mf.id ASC))[1] AS first_seen_scan_run_id
				FROM target_episode target JOIN media_files mf
				  ON mf.episode_id = target.episode_id AND mf.media_folder_id = target.media_folder_id
				 AND mf.missing_since IS NULL
				GROUP BY target.episode_id, target.media_folder_id
			), inserted AS (
				INSERT INTO episode_libraries (episode_id, media_folder_id, first_seen_at, first_seen_scan_run_id)
				SELECT episode_id, media_folder_id, first_seen_at, first_seen_scan_run_id FROM candidate
				ON CONFLICT (episode_id, media_folder_id) DO NOTHING
				RETURNING episode_id, first_seen_at
			)
			UPDATE media_items mi
			SET latest_episode_added_at = GREATEST(mi.latest_episode_added_at, sub.latest_added)
			FROM (
				SELECT e.series_id, MAX(i.first_seen_at) AS latest_added
				FROM inserted i JOIN episodes e ON e.content_id = i.episode_id GROUP BY e.series_id
			) sub
			WHERE mi.content_id = sub.series_id AND mi.type = 'series'
		`, folderID)
		return scanCostOwnTag(tag, err, "UPDATE 1")
	}, map[string]scanCostTuples{
		"episode_libraries": {ins: 1},
		"media_items":       {upd: 1},
		// legitimate: membership materialization via migrations/sql/142_episode_catalog_entries.sql:594-597,
		// whose refresh function inserts at migrations/sql/20261008034854_skip_unchanged_episode_catalog_refresh.sql:213.
		"episode_catalog_entries": {ins: 1},
	})
}

// scanCostTitle is the provider identity of one recipe movie or show.
type scanCostTitle struct {
	title string
	year  int
	kind  string
	id    string
}

func (title scanCostTitle) ids() map[string]string {
	if title.kind == "series" {
		return map[string]string{"tvdb": title.id}
	}
	return map[string]string{"tmdb": title.id}
}

type scanCostProvider struct {
	titles []scanCostTitle
	calls  atomic.Int64
}

func (provider *scanCostProvider) Slug() string {
	provider.calls.Add(1)
	return "tmdb"
}

func (provider *scanCostProvider) Name() string {
	provider.calls.Add(1)
	return "Scan cost provider"
}

func (provider *scanCostProvider) ForTypes() []string {
	provider.calls.Add(1)
	return []string{"movie", "series"}
}

func (provider *scanCostProvider) find(kind string, ids map[string]string) (scanCostTitle, bool) {
	for _, title := range provider.titles {
		if title.kind != kind {
			continue
		}
		for key, id := range title.ids() {
			if ids[key] == id {
				return title, true
			}
		}
	}
	return scanCostTitle{}, false
}

func (provider *scanCostProvider) Search(_ context.Context, query metadata.SearchQuery) ([]metadata.SearchResult, error) {
	provider.calls.Add(1)
	result := func(title scanCostTitle) []metadata.SearchResult {
		return []metadata.SearchResult{{
			Name: title.title, Year: title.year, ProviderIDs: title.ids(), Provider: "tmdb",
		}}
	}
	if len(query.ProviderIDs) > 0 {
		if title, ok := provider.find(query.ContentType, query.ProviderIDs); ok {
			return result(title), nil
		}
		return nil, nil
	}
	for _, title := range provider.titles {
		if title.kind == query.ContentType && title.year == query.Year && strings.EqualFold(title.title, query.Title) {
			return result(title), nil
		}
	}
	return nil, nil
}

func (provider *scanCostProvider) GetMetadata(_ context.Context, req metadata.MetadataRequest) (*metadata.MetadataResult, error) {
	provider.calls.Add(1)
	title, ok := provider.find(req.ContentType, req.ProviderIDs)
	if !ok {
		return &metadata.MetadataResult{}, nil
	}
	return &metadata.MetadataResult{
		HasMetadata: true, Title: title.title, Year: title.year,
		ReleaseDate: fmt.Sprintf("%d-06-01", title.year), ProviderIDs: title.ids(),
	}, nil
}

func (provider *scanCostProvider) GetSeasons(_ context.Context, req metadata.SeasonsRequest) ([]metadata.SeasonResult, error) {
	provider.calls.Add(1)
	if _, ok := provider.find("series", req.ProviderIDs); !ok {
		return nil, nil
	}
	return []metadata.SeasonResult{
		{SeasonNumber: 1, Title: "Season 1"},
		{SeasonNumber: 2, Title: "Season 2"},
	}, nil
}

func (provider *scanCostProvider) GetEpisodes(_ context.Context, req metadata.EpisodesRequest) ([]metadata.EpisodeResult, error) {
	provider.calls.Add(1)
	if _, ok := provider.find("series", req.ProviderIDs); !ok || req.SeasonNumber < 1 || req.SeasonNumber > 2 {
		return nil, nil
	}
	episodes := make([]metadata.EpisodeResult, 0, 4)
	for episode := 1; episode <= 4; episode++ {
		episodes = append(episodes, metadata.EpisodeResult{
			SeasonNumber: req.SeasonNumber, EpisodeNumber: episode, Title: fmt.Sprintf("Episode %d", episode),
		})
	}
	return episodes, nil
}

type scanCostTree struct {
	kind      string
	wantFiles int
	wantItems int
	roots     []string
	files     []string
	titles    []scanCostTitle
	versions  []string
	parts     []string
	rejected  string
}

func scanCostCode(index int) string {
	// Only these three-letter codes are release tokens in naming's variant
	// and root-inference expressions; skipping them preserves literal titles.
	for candidate, accepted := 0, 0; ; candidate++ {
		code := string([]byte{'A' + byte(candidate/676), 'a' + byte(candidate/26%26), 'a' + byte(candidate%26)})
		switch strings.ToLower(code) {
		case "aac", "dts", "hdr", "sdr", "vff":
			continue
		}
		if accepted == index {
			return code
		}
		accepted++
	}
}

func scanCostWriteMedia(t testing.TB, tree *scanCostTree, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	err = file.Truncate(1<<20 + int64(len(tree.files)))
	if err := errors.Join(err, file.Close()); err != nil {
		t.Fatal(err)
	}
	mtime := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	tree.files = append(tree.files, path)
}

func scanCostBuildTree(t testing.TB, kind string) *scanCostTree {
	t.Helper()
	base := t.TempDir()
	tree := &scanCostTree{kind: kind, roots: []string{filepath.Join(base, kind+"-a"), filepath.Join(base, kind+"-b")}}
	for _, root := range tree.roots {
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if kind == "movies" {
		tree.wantFiles, tree.wantItems = 42, 40
		scanCostBuildMovies(t, tree)
	} else {
		tree.wantFiles, tree.wantItems = 47, 6
		scanCostBuildSeries(t, tree)
	}
	return tree
}

func scanCostBuildMovies(t testing.TB, tree *scanCostTree) {
	t.Helper()
	for k := range 40 {
		title := scanCostTitle{title: "Cost Movie " + scanCostCode(k), year: 1990 + k%30, kind: "movie", id: strconv.Itoa(100000 + k)}
		if k == 39 {
			title.title, title.year = "Rejected Movie", 2004
		}
		tree.titles = append(tree.titles, title)
		stem := fmt.Sprintf("%s (%d)", title.title, title.year)
		folder := stem
		if k < 33 || k >= 37 {
			folder += " {tmdb-" + title.id + "}"
		}
		dir := filepath.Join(tree.roots[k%2], folder)
		suffixes := []string{""}
		switch k {
		case 37:
			suffixes = []string{" - 1080p", " - 2160p"}
		case 38:
			suffixes = []string{" - cd1", " - cd2"}
		}
		for _, suffix := range suffixes {
			path := filepath.Join(dir, stem+suffix+".mp4")
			scanCostWriteMedia(t, tree, path)
			switch k {
			case 37:
				tree.versions = append(tree.versions, path)
			case 38:
				tree.parts = append(tree.parts, path)
			case 39:
				tree.rejected = path
			}
		}
	}
}

func scanCostBuildSeries(t testing.TB, tree *scanCostTree) {
	t.Helper()
	for k := range 6 {
		title := scanCostTitle{title: "Cost Show " + scanCostCode(k), year: 1990 + k%30, kind: "series", id: strconv.Itoa(200000 + k)}
		tree.titles = append(tree.titles, title)
		stem := fmt.Sprintf("%s (%d)", title.title, title.year)
		folder := stem
		if k < 5 {
			folder += " {tvdb-" + title.id + "}"
		}
		for season := 1; season <= 2; season++ {
			for episode := 1; episode <= 4; episode++ {
				if k == 0 && season == 1 && episode == 2 {
					continue
				}
				suffix := fmt.Sprintf(" - S%02dE%02d", season, episode)
				if k == 0 && season == 1 && episode == 1 {
					suffix += "-E02"
				}
				path := filepath.Join(tree.roots[k%2], folder, fmt.Sprintf("Season %02d", season), stem+suffix+".mp4")
				scanCostWriteMedia(t, tree, path)
			}
		}
	}
}

type scanCostProbe struct {
	path  string
	count string
}

func scanCostFakeProbe(t testing.TB) scanCostProbe {
	t.Helper()
	fixture, err := os.ReadFile("testdata/scan_cost_ffprobe.json")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	probe := scanCostProbe{path: filepath.Join(dir, "ffprobe"), count: filepath.Join(dir, "calls")}
	if err := os.WriteFile(probe.count, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'" }
	script := "#!/bin/sh\nprintf x >> " + quote(probe.count) + `
for arg do
  case "$arg" in
    *Rejected*) printf '%s\n' 'Invalid data found when processing input' >&2; exit 1 ;;
  esac
done
cat <<'SCAN_COST_FFPROBE'
` + string(fixture) + "\nSCAN_COST_FFPROBE\n"
	// execve fails with ETXTBSY if a concurrent fork inherits the writing fd.
	// Hold the fork lock until the executable is closed, as writeFakeTool does.
	syscall.ForkLock.Lock()
	err = os.WriteFile(probe.path, []byte(script), 0o755)
	syscall.ForkLock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	return probe
}

func (probe scanCostProbe) calls(t testing.TB) int64 {
	t.Helper()
	info, err := os.Stat(probe.count)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

type scanCostMatcher struct {
	*metadata.MatchWorker
	polls atomic.Int64
}

func (matcher *scanCostMatcher) ProcessBatchByFolderAndPathPrefix(ctx context.Context, folderID int, prefix string, before time.Time) (int, error) {
	processed, err := matcher.MatchWorker.ProcessBatchByFolderAndPathPrefix(
		context.WithValue(ctx, scanCostPhaseKey{}, "drain"), folderID, prefix, before)
	matcher.polls.Add(1)
	return processed, err
}

func (matcher *scanCostMatcher) ProcessAllByFolderAndPathPrefix(ctx context.Context, folderID int, prefix string, before time.Time) (int, error) {
	return matcher.MatchWorker.ProcessAllByFolderAndPathPrefix(
		context.WithValue(ctx, scanCostPhaseKey{}, "match"), folderID, prefix, before)
}

func (matcher *scanCostMatcher) RetryUnmatchedItemsByFolderAndPathPrefix(ctx context.Context, folderID int, prefix string) (int, int, error) {
	return matcher.MatchWorker.RetryUnmatchedItemsByFolderAndPathPrefix(
		context.WithValue(ctx, scanCostPhaseKey{}, "match"), folderID, prefix)
}

type scanCostScanner struct {
	*scanner.Scanner
	matcher *scanCostMatcher
}

func (scan *scanCostScanner) FinalizeVariantsByPathPrefix(ctx context.Context, folder *models.MediaFolder, prefix string) error {
	return scan.Scanner.FinalizeVariantsByPathPrefix(context.WithValue(ctx, scanCostPhaseKey{}, "finalize"), folder, prefix)
}

func (scan *scanCostScanner) ScanFolder(ctx context.Context, folder *models.MediaFolder) (*scanner.ScanResult, error) {
	result, err := scan.Scanner.ScanFolder(ctx, folder)
	if err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for scan.matcher.polls.Load() < 1 {
		select {
		case <-ctx.Done():
			return result, fmt.Errorf("scan-cost first drainer poll: %w", ctx.Err())
		case <-ticker.C:
		}
	}
	return result, nil
}

type scanCostHarness struct {
	executor *libraryingest.Executor
	matcher  *scanCostMatcher
}

// scanCostNewHarness wires each ingest on its own pool. It omits the
// availability detector, event bus, realtime hub, image cacher, person and
// stale-ID repositories, and plugin resolver (stub providers instead).
func scanCostNewHarness(pool *pgxpool.Pool, probe scanCostProbe, provider *scanCostProvider) scanCostHarness {
	files := scanner.NewFileRepository(pool)
	folders := catalog.NewFolderRepository(pool)
	skipped := metadata.NewSkippedRootRepository(pool)
	service := metadata.NewMetadataService(metadata.NewChainRepository(pool), nil, nil,
		catalog.NewItemRepository(pool), catalog.NewProviderIDRepository(pool),
		catalog.NewEpisodeRepository(pool), catalog.NewSeasonRepository(pool),
		catalog.NewLibraryItemRepository(pool), folders,
		nil, files, skipped, nil, catalog.NewRootClaimRepository(pool))
	service.SetLibraryRootResolver(folders)
	service.SetLocalSidecarStore(metadata.NewLocalSidecarRepository(pool))
	metadata.MatchWithProvidersForTest(service, "en", provider)

	movieQueue := metadata.NewMovieMatchQueueRepository(pool, files)
	seriesQueue := metadata.NewSeriesRootMatchQueueRepository(pool)
	worker := metadata.NewMatchWorker(service, files, 8, 500, 30*time.Second)
	worker.SetMovieFileClaimer(movieQueue)
	worker.SetSeriesRootClaimer(seriesQueue, true)
	matcher := &scanCostMatcher{MatchWorker: worker}
	scan := scanner.NewScanner(files, probe.path, nil, 8, true, 24*time.Hour)
	scan.SetMetadataQueueProducer(metadata.NewMatchQueueCoordinator(movieQueue, seriesQueue))
	scan.SetMovieQueueSyncer(movieQueue)
	scan.SetSeriesQueueSyncer(seriesQueue)
	executor := libraryingest.NewExecutor(&scanCostScanner{Scanner: scan, matcher: matcher}, matcher, folders, skipped, nil, nil)
	executor.SetLocalSidecarChecker(service)
	return scanCostHarness{executor: executor, matcher: matcher}
}

type scanCostPass struct {
	measurement scanCostMeasurement
	result      *libraryingest.Result
	err         error
	polls       int64
	probes      int64
	providers   int64
}

func scanCostIngest(t testing.TB, config *pgxpool.Config, setup scanCostOperation, folder **models.MediaFolder,
	probe scanCostProbe, provider *scanCostProvider,
) scanCostPass {
	t.Helper()
	var pass scanCostPass
	beforeProbes, beforeProviders := probe.calls(t), provider.calls.Load()
	pass.measurement, pass.err = scanCostMeasure(t.Context(), config, setup, func(ctx context.Context, pool *pgxpool.Pool) error {
		harness := scanCostNewHarness(pool, probe, provider)
		var err error
		pass.result, err = harness.executor.IngestFolder(ctx, *folder)
		pass.polls = harness.matcher.polls.Load()
		return err
	})
	pass.probes = probe.calls(t) - beforeProbes
	pass.providers = provider.calls.Load() - beforeProviders
	return pass
}

func scanCostCheckValue(t testing.TB, check string, observed, want int64) {
	t.Helper()
	if observed != want {
		t.Errorf("scan-cost %s: observed %d, want %d", check, observed, want)
	}
}

func scanCostCheckOutcome(t testing.TB, pass scanCostPass, noop bool, files int) {
	t.Helper()
	if pass.err != nil {
		t.Errorf("scan-cost measured ingest error: observed %v, want nil", pass.err)
	}
	if pass.result == nil || pass.result.ScanResult == nil {
		t.Error("scan-cost scan result: observed nil, want non-nil")
		return
	}
	result := pass.result.ScanResult
	scanCostCheckValue(t, "scan errors", int64(result.Errors), 0)
	if noop {
		scanCostCheckValue(t, "no-op New", int64(result.New), 0)
		scanCostCheckValue(t, "no-op Updated", int64(result.Updated), 0)
		scanCostCheckValue(t, "no-op Unchanged", int64(result.Unchanged), int64(files))
		scanCostCheckValue(t, "no-op Missing", int64(result.Missing), 0)
	}
}

func scanCostCheckProbeState(t testing.TB, tree *scanCostTree, files []*models.MediaFile) {
	t.Helper()
	byPath := make(map[string]*models.MediaFile, len(files))
	for _, file := range files {
		byPath[file.FilePath] = file
	}
	scanCostCheckValue(t, "pass-one catalog files", int64(len(files)), int64(tree.wantFiles))
	for _, path := range tree.files {
		file := byPath[path]
		if file == nil {
			t.Errorf("scan-cost pass-one file %s: observed absent, want present", path)
			continue
		}
		if path == tree.rejected {
			if file.ProbeFailedAt == nil {
				t.Errorf("scan-cost rejected file %s probe_failed_at: observed nil, want set", path)
			}
			continue
		}
		if file.ProbeUpdatedAt == nil {
			t.Errorf("scan-cost file %s probe_updated_at: observed nil, want set", path)
		}
		if scanner.NeedsCriticalProbeRepair(file) {
			t.Errorf("scan-cost file %s NeedsCriticalProbeRepair: observed true, want false", path)
		}
	}
}

func scanCostCheckMatches(t testing.TB, ctx context.Context, pool *pgxpool.Pool, folderID int, tree *scanCostTree) {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT mf.file_path, COALESCE(mi.content_id, ''), COALESCE(mi.status, '')
		FROM media_files mf LEFT JOIN episodes e ON e.content_id = mf.episode_id
		LEFT JOIN media_items mi ON mi.content_id = COALESCE(e.series_id, mf.content_id)
		WHERE mf.media_folder_id = $1`, folderID)
	if err != nil {
		t.Errorf("scan-cost pass-one matches: %v", err)
		return
	}
	defer rows.Close()
	items := make(map[string]string)
	statuses := make(map[string]string)
	distinct := make(map[string]struct{})
	for rows.Next() {
		var path, contentID, status string
		if err := rows.Scan(&path, &contentID, &status); err != nil {
			t.Errorf("scan-cost pass-one match row: %v", err)
			return
		}
		items[path], statuses[path] = contentID, status
		if contentID != "" {
			distinct[contentID] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		t.Errorf("scan-cost pass-one match rows: %v", err)
	}
	for _, path := range tree.files {
		if items[path] == "" || statuses[path] != "matched" {
			t.Errorf("scan-cost pass-one match %s: observed item=%q status=%q, want nonempty matched item", path, items[path], statuses[path])
		}
	}
	scanCostCheckValue(t, "pass-one distinct items", int64(len(distinct)), int64(tree.wantItems))
	for _, group := range [][]string{tree.versions, tree.parts} {
		if len(group) == 2 && (items[group[0]] == "" || items[group[0]] != items[group[1]]) {
			t.Errorf("scan-cost pass-one shared item %v: observed %q and %q, want same nonempty item", group, items[group[0]], items[group[1]])
		}
	}
}

func scanCostCheckFirst(t testing.TB, config *pgxpool.Config, folder *models.MediaFolder, tree *scanCostTree, pass scanCostPass) {
	t.Helper()
	scanCostCheckOutcome(t, pass, false, tree.wantFiles)
	scanCostCheckValue(t, "recipe files", int64(len(tree.files)), int64(tree.wantFiles))
	scanCostCheckValue(t, "recipe items", int64(len(tree.titles)), int64(tree.wantItems))
	scanCostCheckValue(t, "pass-one probes", pass.probes, int64(tree.wantFiles))
	scanCostCheckValue(t, "pass-one media_files inserts", pass.measurement.writes["media_files"].ins, int64(tree.wantFiles))
	pool, err := scanCostOpenPool(t.Context(), config, nil)
	if err != nil {
		t.Errorf("scan-cost pass-one check pool: %v", err)
		return
	}
	defer pool.Close()
	files, err := scanner.NewFileRepository(pool).GetByFolder(t.Context(), folder.ID)
	if err != nil {
		t.Errorf("scan-cost pass-one files: %v", err)
	} else {
		scanCostCheckProbeState(t, tree, files)
	}
	scanCostCheckMatches(t, t.Context(), pool, folder.ID, tree)
}

func scanCostNoopWrites(kind string) map[string]scanCostTuples {
	if kind == "movies" {
		return map[string]scanCostTuples{
			// legitimate: 1 last-scanned update; debt: 1 warning rewrite.
			"media_folders": {upd: 2},
			// debt: replace unchanged group-location pairs.
			"media_group_locations": {ins: 40, del: 40},
			// debt (unpredicted): internal/metadata/movie_match_queue_repo.go:211.
			"movie_match_queue": {del: 4},
			// debt: replace unchanged observed locations.
			"observed_media_locations": {ins: 40, del: 40},
			// debt: replace unchanged groups.
			"scanned_media_groups": {ins: 40, del: 40},
			// debt: rewrite root last-seen state.
			"scanned_media_roots": {upd: 40},
			// debt: rewrite unchanged untagged roots.
			"skipped_media_roots": {upd: 4},
		}
	}
	return map[string]scanCostTuples{
		// legitimate: 1 last-scanned update; debt: 1 warning rewrite.
		"media_folders": {upd: 2},
		// debt: replace unchanged group-location pairs.
		"media_group_locations": {ins: 6, del: 6},
		// debt: replace unchanged observed locations.
		"observed_media_locations": {ins: 6, del: 6},
		// debt: replace unchanged groups.
		"scanned_media_groups": {ins: 6, del: 6},
		// debt: rewrite root last-seen state.
		"scanned_media_roots": {upd: 6},
		// debt: rewrite the unchanged untagged root.
		"skipped_media_roots": {upd: 1},
	}
}

func scanCostCheckNoop(t testing.TB, tree *scanCostTree, pass scanCostPass) {
	t.Helper()
	scanCostCheckOutcome(t, pass, true, tree.wantFiles)
	scanCostCheckValue(t, "no-op probes", pass.probes, 0)
	scanCostCheckValue(t, "no-op provider calls", pass.providers, 0)
	scanCostCheckWrites(t, pass.measurement.writes, scanCostNoopWrites(tree.kind))
	tracer := pass.measurement.tracer
	if tracer == nil {
		t.Errorf("scan-cost no-op tracer: observed nil, want non-nil; bucket=unavailable wall=%s polls=%d exempt-cancellations=unavailable", pass.measurement.wall, pass.polls)
		return
	}
	t.Logf("scan-cost no-op bucket=%d wall=%s polls=%d exempt-cancellations=%d",
		tracer.folderReads, pass.measurement.wall, pass.polls, tracer.exemptCancellations)
	// A measured no-op pass pins variant reads across both roots, scoped matching plus retry,
	// and the remaining ingest statements after removing the folder-read bucket.
	wants := map[string]int64{"finalize": 4, "match": 14, "other": 102}
	if tree.kind == "movies" {
		wants["other"] = 272
	}
	for _, phase := range []string{"finalize", "match", "other"} {
		count := tracer.statements[phase]
		if phase == "other" {
			count -= tracer.folderReads
		}
		scanCostCheckValue(t, "no-op "+phase+" statements", count, wants[phase])
	}
	// A measured no-op pass of each recipe runs below 1 s: S is six synchronous folder reads.
	// The bucket is bounded, not exact: each 1 s watcher tick adds up to two.
	const synchronousFolderReads int64 = 6
	maxFolderReads := synchronousFolderReads + 2*int64(pass.measurement.wall/time.Second)
	if tracer.folderReads < synchronousFolderReads || tracer.folderReads > maxFolderReads {
		t.Errorf("scan-cost no-op folder-read bucket: observed %d, want [%d, %d]", tracer.folderReads, synchronousFolderReads, maxFolderReads)
	}
	if pass.polls < 1 {
		t.Errorf("scan-cost no-op drain polls: observed %d, want >= 1; drain statements=%d", pass.polls, tracer.statements["drain"])
	}
	// A measured no-op pass records six drain statements over two polls for each recipe.
	const statementsPerPoll int64 = 3
	scanCostCheckValue(t, "no-op drain statements", tracer.statements["drain"], pass.polls*statementsPerPoll)
}

func scanCostGate(t *testing.T, kind string) {
	t.Helper()
	config := scanCostClone(t)
	tree := scanCostBuildTree(t, kind)
	probe := scanCostFakeProbe(t)
	provider := &scanCostProvider{titles: tree.titles}
	var folder *models.MediaFolder
	setup := func(ctx context.Context, pool *pgxpool.Pool) error {
		folderType := "movies"
		if kind == "series" {
			folderType = "tv"
		}
		var err error
		folder, err = catalog.NewFolderRepository(pool).Create(ctx, catalog.CreateFolderInput{
			Name: "Scan cost " + kind, Type: folderType, Paths: tree.roots,
		})
		return err
	}
	first := scanCostIngest(t, config, setup, &folder, probe, provider)
	if folder == nil {
		t.Fatalf("scan-cost library setup: observed folder=nil error=%v, want a library with both roots", first.err)
	}
	scanCostCheckFirst(t, config, folder, tree, first)
	if t.Failed() {
		t.FailNow()
	}
	second := scanCostIngest(t, config, nil, &folder, probe, provider)
	scanCostCheckNoop(t, tree, second)
}

// TestScanCostNoopRescanDB measures a whole-library ingest twice on owned
// movie and series databases. Pass two pins outcomes, probes, provider calls,
// per-table writes, and per-phase statements, including concurrent drain polls.
func TestScanCostNoopRescanDB(t *testing.T) {
	t.Run("movies", func(t *testing.T) {
		scanCostGate(t, "movies")
	})
	t.Run("series", func(t *testing.T) {
		scanCostGate(t, "series")
	})
}
