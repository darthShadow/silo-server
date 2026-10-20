package metadata_test

import (
	"path/filepath"
	"testing"
)

func TestScanClearsSkippedRowOfTaggedRootDB(t *testing.T) {
	for _, mode := range []string{"library", "subtree"} {
		t.Run(mode, func(t *testing.T) {
			movie := catalogMovie{tmdb: "970013", title: "Tagged Snapshot", year: 2020}
			h := newIngestHarness(t, &stubTMDB{movies: []catalogMovie{movie}})
			folder := h.movieLibrary(t, t.TempDir())
			root := filepath.Join(folder.Paths[0], "Tagged Snapshot (2020) [tmdb-970013]")
			file := writeMovieFile(t, root, "Tagged Snapshot (2020).mp4")
			// Match before seeding so the second ingest tests executor cleanup
			// without the matcher clearing the skipped row itself.
			if _, err := h.executor.IngestFolder(t.Context(), folder); err != nil {
				t.Fatalf("initial library ingest: %v", err)
			}
			var contentID string
			if err := h.pool.QueryRow(t.Context(), `SELECT COALESCE(content_id, '') FROM media_files WHERE media_folder_id = $1 AND file_path = $2`, folder.ID, file).Scan(&contentID); err != nil {
				t.Fatalf("read file content_id after initial ingest: %v", err)
			}
			if contentID == "" {
				t.Fatalf("file content_id after initial ingest = %q, want nonempty", contentID)
			}
			if _, err := h.pool.Exec(t.Context(), `
				INSERT INTO skipped_media_roots (media_folder_id, root_path, reason, sample_file_path, file_count)
				VALUES ($1, $2, 'missing_folder_ids', $3, 1)`, folder.ID, root, file); err != nil {
				t.Fatalf("seed skipped root: %v", err)
			}
			var before int
			if err := h.pool.QueryRow(t.Context(), `SELECT count(*) FROM skipped_media_roots WHERE media_folder_id = $1 AND root_path = $2`, folder.ID, root).Scan(&before); err != nil {
				t.Fatalf("read seeded skipped root: %v", err)
			}
			if before != 1 {
				t.Fatalf("skipped rows before ingest = %d, want 1", before)
			}
			if mode == "library" {
				if _, err := h.executor.IngestFolder(t.Context(), folder); err != nil {
					t.Fatalf("ingest library: %v", err)
				}
			} else if _, err := h.executor.IngestSubtree(t.Context(), folder, root); err != nil {
				t.Fatalf("ingest subtree: %v", err)
			}
			var after int
			if err := h.pool.QueryRow(t.Context(), `SELECT count(*) FROM skipped_media_roots WHERE media_folder_id = $1 AND root_path = $2`, folder.ID, root).Scan(&after); err != nil {
				t.Fatalf("read skipped root after ingest: %v", err)
			}
			if after != 0 {
				t.Errorf("skipped rows after %s ingest = %d, want 0", mode, after)
			}
		})
	}
}
