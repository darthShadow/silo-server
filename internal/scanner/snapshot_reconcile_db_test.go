package scanner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Silo-Server/silo-server/internal/catalog"
	"github.com/Silo-Server/silo-server/internal/models"
)

type snapshotReconcileRow struct {
	columns   string
	xmin      string
	firstSeen time.Time
	lastSeen  time.Time
	fileCount int
}

func snapshotReconcileLibrary(t *testing.T, roots ...string) (*pgxpool.Pool, *models.MediaFolder, *Scanner) {
	t.Helper()
	pool := newDeadRootTestPool(t)
	folder, err := catalog.NewFolderRepository(pool).Create(t.Context(), catalog.CreateFolderInput{
		Name: "Snapshot reconciliation", Type: "movies", Paths: roots,
	})
	if err != nil {
		t.Fatalf("create library: %v", err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DELETE FROM media_folders WHERE id = $1`, folder.ID); err != nil {
			t.Errorf("delete library: %v", err)
		}
	})
	return pool, folder, NewScanner(NewFileRepository(pool), "", nil, 2, true, 24*time.Hour)
}

func snapshotReconcileMovie(t *testing.T, root, title string) string {
	t.Helper()
	dir := filepath.Join(root, title)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, title+".mp4"), []byte("video"), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func snapshotReconcileScan(t *testing.T, scan *Scanner, folder *models.MediaFolder) *ScanResult {
	t.Helper()
	result, err := scan.ScanFolder(t.Context(), folder)
	if err != nil {
		t.Fatalf("scan library: %v", err)
	}
	if result.Errors != 0 {
		t.Fatalf("scan errors = %d, want 0", result.Errors)
	}
	return result
}

func snapshotReconcileRead(t *testing.T, pool *pgxpool.Pool, folderID int, path string, wantRows int) map[string]snapshotReconcileRow {
	t.Helper()
	rows, err := pool.Query(t.Context(), `
		SELECT 'group', to_jsonb(s)::text, s.xmin::text, first_seen_at, last_seen_at, observed_file_count
		FROM scanned_media_groups s WHERE media_folder_id = $1 AND sample_observed_root_path = $2
		UNION ALL
		SELECT 'location', to_jsonb(s)::text, s.xmin::text, first_seen_at, last_seen_at, observed_file_count
		FROM observed_media_locations s WHERE media_folder_id = $1 AND observed_root_path = $2
		UNION ALL
		SELECT 'group_location', to_jsonb(s)::text, s.xmin::text, first_seen_at, last_seen_at, 0
		FROM media_group_locations s WHERE media_folder_id = $1 AND observed_root_path = $2`, folderID, path)
	if err != nil {
		t.Fatalf("read snapshots for %s: %v", path, err)
	}
	defer rows.Close()
	snapshots := make(map[string]snapshotReconcileRow)
	for rows.Next() {
		var table string
		var row snapshotReconcileRow
		if err := rows.Scan(&table, &row.columns, &row.xmin, &row.firstSeen, &row.lastSeen, &row.fileCount); err != nil {
			t.Fatalf("read snapshot row: %v", err)
		}
		if _, exists := snapshots[table]; exists {
			t.Fatalf("%s at %s: more than one row, want at most one", table, path)
		}
		snapshots[table] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate snapshot rows: %v", err)
	}
	if len(snapshots) != wantRows {
		t.Errorf("snapshots at %s = %v, want %d rows", path, snapshots, wantRows)
	}
	return snapshots
}

func snapshotReconcileCheckFirstSeen(t *testing.T, before, after map[string]snapshotReconcileRow) {
	t.Helper()
	for table, previous := range before {
		if current, ok := after[table]; !ok || !current.firstSeen.Equal(previous.firstSeen) {
			t.Errorf("%s first_seen_at = %s (present %t), want %s", table, current.firstSeen, ok, previous.firstSeen)
		}
	}
}

func snapshotReconcileCheckUnchanged(t *testing.T, table string, before, after map[string]snapshotReconcileRow) {
	t.Helper()
	previous, hadRow := before[table]
	current, hasRow := after[table]
	if !hadRow || !hasRow || current.columns != previous.columns || current.xmin != previous.xmin {
		t.Errorf("%s row = %s xmin=%s (present %t), want %s xmin=%s (present %t)",
			table, current.columns, current.xmin, hasRow, previous.columns, previous.xmin, hadRow)
	}
}

func TestRescanKeepsFirstSeenAndAdvancesLastSeenDB(t *testing.T) {
	root := t.TempDir()
	x := snapshotReconcileMovie(t, root, "Alpha (2020)")
	y := snapshotReconcileMovie(t, root, "Beta (2021)")
	pool, folder, scan := snapshotReconcileLibrary(t, root)
	snapshotReconcileScan(t, scan, folder)
	before := make(map[string]map[string]snapshotReconcileRow)
	rootTimes := make(map[string]time.Time)
	for _, path := range []string{x, y} {
		before[path] = snapshotReconcileRead(t, pool, folder.ID, path, 3)
		var seen time.Time
		if err := pool.QueryRow(t.Context(), `SELECT last_seen_at FROM scanned_media_roots WHERE media_folder_id = $1 AND root_path = $2`, folder.ID, path).Scan(&seen); err != nil {
			t.Fatalf("read root last_seen_at: %v", err)
		}
		rootTimes[path] = seen
	}
	snapshotReconcileScan(t, scan, folder)
	for _, path := range []string{x, y} {
		after := snapshotReconcileRead(t, pool, folder.ID, path, 3)
		snapshotReconcileCheckFirstSeen(t, before[path], after)
		if !after["group"].lastSeen.After(before[path]["group"].lastSeen) {
			t.Errorf("group %s last_seen_at = %s, want greater than %s", path, after["group"].lastSeen, before[path]["group"].lastSeen)
		}
		var seen time.Time
		if err := pool.QueryRow(t.Context(), `SELECT last_seen_at FROM scanned_media_roots WHERE media_folder_id = $1 AND root_path = $2`, folder.ID, path).Scan(&seen); err != nil {
			t.Fatalf("read root last_seen_at: %v", err)
		}
		if !seen.After(rootTimes[path]) {
			t.Errorf("root %s last_seen_at = %s, want greater than %s", path, seen, rootTimes[path])
		}
	}
}

func TestPartialWalkKeepsUnseenSnapshotsDB(t *testing.T) {
	root := t.TempDir()
	x := snapshotReconcileMovie(t, root, "Alpha (2020)")
	y := snapshotReconcileMovie(t, root, "Beta (2021)")
	pool, folder, scan := snapshotReconcileLibrary(t, root)
	snapshotReconcileScan(t, scan, folder)
	xBefore := snapshotReconcileRead(t, pool, folder.ID, x, 3)
	yBefore := snapshotReconcileRead(t, pool, folder.ID, y, 3)
	if err := os.RemoveAll(y); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "missing"), filepath.Join(root, "dangling")); err != nil {
		t.Fatal(err)
	}
	snapshotReconcileScan(t, scan, folder)
	yAfter := snapshotReconcileRead(t, pool, folder.ID, y, 3)
	for _, table := range []string{"group", "location", "group_location"} {
		snapshotReconcileCheckUnchanged(t, table, yBefore, yAfter)
	}
	xAfter := snapshotReconcileRead(t, pool, folder.ID, x, 3)
	for _, table := range []string{"location", "group_location"} {
		snapshotReconcileCheckUnchanged(t, table, xBefore, xAfter)
	}
	if !xAfter["group"].lastSeen.After(xBefore["group"].lastSeen) {
		t.Errorf("seen group last_seen_at = %s, want greater than %s", xAfter["group"].lastSeen, xBefore["group"].lastSeen)
	}
}

func TestCompleteRescanPrunesUnseenSnapshotsDB(t *testing.T) {
	for _, emptyScope := range []bool{false, true} {
		name := "removed_movie"
		if emptyScope {
			name = "scope_without_media"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			xRoot, yRoot := root, root
			if emptyScope {
				xRoot, yRoot = filepath.Join(root, "R1"), filepath.Join(root, "R2")
			}
			x := snapshotReconcileMovie(t, xRoot, "Alpha (2020)")
			y := snapshotReconcileMovie(t, yRoot, "Beta (2021)")
			roots := []string{xRoot}
			if emptyScope {
				roots = append(roots, yRoot)
			}
			pool, folder, scan := snapshotReconcileLibrary(t, roots...)
			snapshotReconcileScan(t, scan, folder)
			snapshotReconcileRead(t, pool, folder.ID, x, 3)
			snapshotReconcileRead(t, pool, folder.ID, y, 3)
			if err := os.RemoveAll(y); err != nil {
				t.Fatal(err)
			}
			if emptyScope {
				if err := os.WriteFile(filepath.Join(yRoot, "notes.txt"), []byte("notes"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			snapshotReconcileScan(t, scan, folder)
			snapshotReconcileRead(t, pool, folder.ID, y, 0)
			snapshotReconcileRead(t, pool, folder.ID, x, 3)
		})
	}
}

func TestLibraryPathReplacementPrunesRemovedPathSnapshotsDB(t *testing.T) {
	root := t.TempDir()
	aRoot, bRoot, cRoot := filepath.Join(root, "A"), filepath.Join(root, "B"), filepath.Join(root, "C")
	a := snapshotReconcileMovie(t, aRoot, "Alpha (2020)")
	b := snapshotReconcileMovie(t, bRoot, "Beta (2021)")
	c := snapshotReconcileMovie(t, cRoot, "Gamma (2022)")
	pool, folder, scan := snapshotReconcileLibrary(t, aRoot, bRoot)
	snapshotReconcileScan(t, scan, folder)
	aBefore := snapshotReconcileRead(t, pool, folder.ID, a, 3)
	snapshotReconcileRead(t, pool, folder.ID, b, 3)
	paths := []string{aRoot, cRoot}
	if err := catalog.NewFolderRepository(pool).Update(t.Context(), folder.ID, catalog.UpdateFolderInput{Paths: &paths}); err != nil {
		t.Fatalf("replace library paths: %v", err)
	}
	folder.Paths = paths
	snapshotReconcileScan(t, scan, folder)
	snapshotReconcileRead(t, pool, folder.ID, b, 0)
	aAfter := snapshotReconcileRead(t, pool, folder.ID, a, 3)
	snapshotReconcileCheckFirstSeen(t, aBefore, aAfter)
	cBefore := snapshotReconcileRead(t, pool, folder.ID, c, 3)
	if err := os.RemoveAll(cRoot); err != nil {
		t.Fatal(err)
	}
	result := snapshotReconcileScan(t, scan, folder)
	if len(result.UnreachableRoots) != 1 || result.UnreachableRoots[0] != cRoot {
		t.Errorf("unreachable roots = %v, want [%s]", result.UnreachableRoots, cRoot)
	}
	cAfter := snapshotReconcileRead(t, pool, folder.ID, c, 3)
	for _, table := range []string{"group", "location", "group_location"} {
		snapshotReconcileCheckUnchanged(t, table, cBefore, cAfter)
	}
}

func TestRescanRewritesChangedSnapshotsDB(t *testing.T) {
	root := t.TempDir()
	x := snapshotReconcileMovie(t, root, "Alpha (2020)")
	pool, folder, scan := snapshotReconcileLibrary(t, root)
	snapshotReconcileScan(t, scan, folder)
	before := snapshotReconcileRead(t, pool, folder.ID, x, 3)
	if err := os.WriteFile(filepath.Join(x, "Alpha (2020) - 2160p.mp4"), []byte("video version"), 0o644); err != nil {
		t.Fatal(err)
	}
	snapshotReconcileScan(t, scan, folder)
	after := snapshotReconcileRead(t, pool, folder.ID, x, 3)
	snapshotReconcileCheckFirstSeen(t, before, after)
	for _, table := range []string{"group", "location"} {
		if after[table].fileCount != 2 {
			t.Errorf("%s observed_file_count = %d, want 2", table, after[table].fileCount)
		}
	}
}
