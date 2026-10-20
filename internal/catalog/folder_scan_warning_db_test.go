package catalog

import (
	"context"
	"testing"
	"time"
)

func TestClearScanWarningWritesOnlyChangesDB(t *testing.T) {
	pool := newAdvisoryAgeTestPool(t)
	repo := NewFolderRepository(pool)
	code, message := "empty_scan_root", "The root has no media."
	otherMessage := "The root is unreachable."
	at := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	warning := ScanWarning{Code: &code, Message: &message, At: &at}
	otherWarning := ScanWarning{Code: &code, Message: &otherMessage, At: &at}
	for _, tc := range []struct {
		name        string
		stored      ScanWarning
		expected    ScanWarning
		replacement ScanWarning
		allow       bool
		unchanged   bool
	}{
		{name: "already_clear", unchanged: true},
		{name: "clear_resets_allowance", allow: true},
		{name: "matching_warning_clears", stored: warning, expected: warning, allow: true},
		{name: "changed_warning_keeps_row", stored: otherWarning, expected: warning, allow: true, unchanged: true},
		{name: "same_warning_keeps_row", stored: warning, expected: warning, replacement: warning, allow: true, unchanged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			folder, err := repo.Create(t.Context(), CreateFolderInput{Name: "Scan warning", Type: "movies"})
			if err != nil {
				t.Fatalf("create library: %v", err)
			}
			t.Cleanup(func() {
				if _, err := pool.Exec(context.Background(), `DELETE FROM media_folders WHERE id = $1`, folder.ID); err != nil {
					t.Errorf("delete library: %v", err)
				}
			})
			if _, err := pool.Exec(t.Context(), `
				UPDATE media_folders SET scan_warning_code = $2, scan_warning_message = $3,
					scan_warning_at = $4, allow_empty_cleanup_once = $5 WHERE id = $1`,
				folder.ID, tc.stored.Code, tc.stored.Message, tc.stored.At, tc.allow); err != nil {
				t.Fatalf("seed warning: %v", err)
			}
			var before, beforeXmin string
			if err := pool.QueryRow(t.Context(), `SELECT to_jsonb(f)::text, xmin::text FROM media_folders f WHERE id = $1`, folder.ID).Scan(&before, &beforeXmin); err != nil {
				t.Fatalf("read warning before update: %v", err)
			}
			if err := repo.UpdateScanWarningIfUnchanged(t.Context(), folder.ID, tc.expected, tc.replacement); err != nil {
				t.Fatalf("update scan warning: %v", err)
			}
			var after, afterXmin string
			var isClear, allow bool
			if err := pool.QueryRow(t.Context(), `
				SELECT to_jsonb(f)::text, xmin::text,
					scan_warning_code IS NULL AND scan_warning_message IS NULL AND scan_warning_at IS NULL,
					allow_empty_cleanup_once FROM media_folders f WHERE id = $1`, folder.ID).Scan(&after, &afterXmin, &isClear, &allow); err != nil {
				t.Fatalf("read warning after update: %v", err)
			}
			if tc.unchanged {
				if after != before || afterXmin != beforeXmin {
					t.Errorf("folder row = %s xmin=%s, want %s xmin=%s", after, afterXmin, before, beforeXmin)
				}
			} else if !isClear || allow {
				t.Errorf("cleared warning = %t, allow_empty_cleanup_once = %t, want true, false", isClear, allow)
			}
		})
	}
}
