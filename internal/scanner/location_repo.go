package scanner

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Silo-Server/silo-server/internal/models"
	"github.com/Silo-Server/silo-server/internal/pathscope"
)

type ObservedLocationRepository struct {
	pool *pgxpool.Pool
}

// observedLocationUpsertSQL writes an existing row only when a content column
// changed, so last_seen_at marks the last change, not the last sighting.
const observedLocationUpsertSQL = `
	INSERT INTO observed_media_locations (
		media_folder_id, observed_root_path, location_type, sample_file_path,
		observed_file_count, content_group_count, primary_group_key_version, primary_content_group_key,
		state, evidence_json, first_seen_at, last_seen_at
	)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, COALESCE($10, '{}'::jsonb), NOW(), NOW())
	ON CONFLICT (media_folder_id, observed_root_path) DO UPDATE SET
		location_type = EXCLUDED.location_type,
		sample_file_path = EXCLUDED.sample_file_path,
		observed_file_count = EXCLUDED.observed_file_count,
		content_group_count = EXCLUDED.content_group_count,
		primary_group_key_version = EXCLUDED.primary_group_key_version,
		primary_content_group_key = EXCLUDED.primary_content_group_key,
		state = EXCLUDED.state,
		evidence_json = EXCLUDED.evidence_json,
		last_seen_at = NOW()
	WHERE (observed_media_locations.location_type, observed_media_locations.sample_file_path,
		observed_media_locations.observed_file_count, observed_media_locations.content_group_count,
		observed_media_locations.primary_group_key_version, observed_media_locations.primary_content_group_key,
		observed_media_locations.state, observed_media_locations.evidence_json)
		IS DISTINCT FROM (EXCLUDED.location_type, EXCLUDED.sample_file_path,
			EXCLUDED.observed_file_count, EXCLUDED.content_group_count,
			EXCLUDED.primary_group_key_version, EXCLUDED.primary_content_group_key,
			EXCLUDED.state, EXCLUDED.evidence_json)
`

func NewObservedLocationRepository(pool *pgxpool.Pool) *ObservedLocationRepository {
	return &ObservedLocationRepository{pool: pool}
}

func (r *ObservedLocationRepository) Get(
	ctx context.Context,
	folderID int,
	observedRootPath string,
) (*models.ObservedMediaLocation, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT media_folder_id, observed_root_path, location_type, sample_file_path,
		       observed_file_count, content_group_count, primary_group_key_version, primary_content_group_key,
		       state, evidence_json, first_seen_at, last_seen_at
		FROM observed_media_locations
		WHERE media_folder_id = $1 AND observed_root_path = $2
	`, folderID, observedRootPath)
	var location models.ObservedMediaLocation
	if err := row.Scan(
		&location.MediaFolderID,
		&location.ObservedRootPath,
		&location.LocationType,
		&location.SampleFilePath,
		&location.ObservedFileCount,
		&location.ContentGroupCount,
		&location.PrimaryGroupKeyVersion,
		&location.PrimaryContentGroupKey,
		&location.State,
		&location.EvidenceJSON,
		&location.FirstSeenAt,
		&location.LastSeenAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("scanning observed media location: %w", err)
	}
	return &location, nil
}

func (r *ObservedLocationRepository) UpsertMany(ctx context.Context, locations []models.ObservedMediaLocation) error {
	if len(locations) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, location := range locations {
		batch.Queue(observedLocationUpsertSQL,
			location.MediaFolderID,
			location.ObservedRootPath,
			location.LocationType,
			location.SampleFilePath,
			location.ObservedFileCount,
			location.ContentGroupCount,
			location.PrimaryGroupKeyVersion,
			location.PrimaryContentGroupKey,
			location.State,
			location.EvidenceJSON,
		)
	}
	results := r.pool.SendBatch(ctx, batch)
	defer results.Close()
	for range locations {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("upserting observed media locations: %w", err)
		}
	}
	return nil
}

func (r *ObservedLocationRepository) ReplaceForFolder(ctx context.Context, folderID int, locations []models.ObservedMediaLocation) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin observed location replace transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, `DELETE FROM observed_media_locations WHERE media_folder_id = $1`, folderID); err != nil {
		return fmt.Errorf("clearing observed media locations: %w", err)
	}

	batch := &pgx.Batch{}
	for _, location := range locations {
		batch.Queue(`
			INSERT INTO observed_media_locations (
				media_folder_id, observed_root_path, location_type, sample_file_path,
				observed_file_count, content_group_count, primary_group_key_version, primary_content_group_key,
				state, evidence_json, first_seen_at, last_seen_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, COALESCE($10, '{}'::jsonb), NOW(), NOW())
		`,
			location.MediaFolderID,
			location.ObservedRootPath,
			location.LocationType,
			location.SampleFilePath,
			location.ObservedFileCount,
			location.ContentGroupCount,
			location.PrimaryGroupKeyVersion,
			location.PrimaryContentGroupKey,
			location.State,
			location.EvidenceJSON,
		)
	}
	results := tx.SendBatch(ctx, batch)
	for range locations {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("inserting observed media locations: %w", err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("closing observed location batch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit observed location replace transaction: %w", err)
	}
	return nil
}

// ReplaceInScope reconciles a folder's locations at or under scopePath with
// locations, the complete set a scan saw there. Unchanged rows are left as
// they are; locations in scope that are not passed are deleted, so an empty
// list clears the scope. The upserts and the delete commit together or not at
// all.
func (r *ObservedLocationRepository) ReplaceInScope(
	ctx context.Context,
	folderID int,
	scopePath string,
	locations []models.ObservedMediaLocation,
) error {
	scopePath = filepath.Clean(scopePath)
	scopeLike := pathscope.PrefixLike(scopePath)

	// One batch keeps the upserts and prune atomic and gives them the same NOW().
	batch := &pgx.Batch{}
	seenPaths := make([]string, 0, len(locations))
	for _, location := range locations {
		seenPaths = append(seenPaths, location.ObservedRootPath)
		batch.Queue(observedLocationUpsertSQL,
			location.MediaFolderID,
			location.ObservedRootPath,
			location.LocationType,
			location.SampleFilePath,
			location.ObservedFileCount,
			location.ContentGroupCount,
			location.PrimaryGroupKeyVersion,
			location.PrimaryContentGroupKey,
			location.State,
			location.EvidenceJSON,
		)
	}
	// The anti-join also prunes the whole scope when no locations were seen.
	batch.Queue(`
		DELETE FROM observed_media_locations t
		WHERE t.media_folder_id = $1
		  AND (t.observed_root_path = $2 OR t.observed_root_path LIKE $3 ESCAPE '\')
		  AND NOT EXISTS (SELECT 1 FROM unnest($4::text[]) AS s(p) WHERE s.p = t.observed_root_path)
	`, folderID, scopePath, scopeLike, seenPaths)
	results := r.pool.SendBatch(ctx, batch)
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("reconciling observed media locations in scope: %w", err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("closing observed location scoped batch: %w", err)
	}
	return nil
}

// DeleteOutsideRoots removes a folder's locations outside every configured root.
// Rows at or under any root survive even when that root is unreachable. An empty
// roots list sends no statement. It returns an error if the delete fails.
func (r *ObservedLocationRepository) DeleteOutsideRoots(ctx context.Context, folderID int, roots []string) error {
	if len(roots) == 0 {
		return nil
	}
	clauses, rootArgs := pathscope.CoverageClauses("observed_root_path", roots, 2)
	args := append([]any{folderID}, rootArgs...)
	_, err := r.pool.Exec(ctx, `
		DELETE FROM observed_media_locations
		WHERE media_folder_id = $1 AND NOT (`+strings.Join(clauses, " OR ")+`)
	`, args...)
	if err != nil {
		return fmt.Errorf("deleting observed media locations outside roots: %w", err)
	}
	return nil
}

type GroupLocationRepository struct {
	pool *pgxpool.Pool
}

// groupLocationUpsertSQL writes an existing row only when is_primary changed,
// so last_seen_at marks the last change, not the last sighting.
const groupLocationUpsertSQL = `
	INSERT INTO media_group_locations (
		media_folder_id, group_key_version, content_group_key, observed_root_path, is_primary, first_seen_at, last_seen_at
	)
	VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
	ON CONFLICT (media_folder_id, group_key_version, content_group_key, observed_root_path) DO UPDATE SET
		is_primary = EXCLUDED.is_primary,
		last_seen_at = NOW()
	WHERE (media_group_locations.is_primary) IS DISTINCT FROM (EXCLUDED.is_primary)
`

func NewGroupLocationRepository(pool *pgxpool.Pool) *GroupLocationRepository {
	return &GroupLocationRepository{pool: pool}
}

func (r *GroupLocationRepository) UpsertMany(ctx context.Context, locations []models.MediaGroupLocation) error {
	if len(locations) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, location := range locations {
		batch.Queue(groupLocationUpsertSQL,
			location.MediaFolderID,
			location.GroupKeyVersion,
			location.ContentGroupKey,
			location.ObservedRootPath,
			location.IsPrimary,
		)
	}
	results := r.pool.SendBatch(ctx, batch)
	defer results.Close()
	for range locations {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("upserting group locations: %w", err)
		}
	}
	return nil
}

func (r *GroupLocationRepository) ReplaceForFolder(ctx context.Context, folderID int, locations []models.MediaGroupLocation) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin group location replace transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, `DELETE FROM media_group_locations WHERE media_folder_id = $1`, folderID); err != nil {
		return fmt.Errorf("clearing group locations: %w", err)
	}

	batch := &pgx.Batch{}
	for _, location := range locations {
		batch.Queue(`
			INSERT INTO media_group_locations (
				media_folder_id, group_key_version, content_group_key, observed_root_path, is_primary, first_seen_at, last_seen_at
			)
			VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
		`,
			location.MediaFolderID,
			location.GroupKeyVersion,
			location.ContentGroupKey,
			location.ObservedRootPath,
			location.IsPrimary,
		)
	}
	results := tx.SendBatch(ctx, batch)
	for range locations {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("inserting group locations: %w", err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("closing group location batch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit group location replace transaction: %w", err)
	}
	return nil
}

// ReplaceInScope reconciles a folder's group locations at or under scopePath
// with locations, the complete set a scan saw there. Unchanged rows are left
// as they are; group locations in scope that are not passed are deleted, so an
// empty list clears the scope. The upserts and the delete commit together or
// not at all.
func (r *GroupLocationRepository) ReplaceInScope(
	ctx context.Context,
	folderID int,
	scopePath string,
	locations []models.MediaGroupLocation,
) error {
	scopePath = filepath.Clean(scopePath)
	scopeLike := pathscope.PrefixLike(scopePath)

	// One batch keeps the upserts and prune atomic and gives them the same NOW().
	batch := &pgx.Batch{}
	seenVersions := make([]int, 0, len(locations))
	seenKeys := make([]string, 0, len(locations))
	seenPaths := make([]string, 0, len(locations))
	for _, location := range locations {
		seenVersions = append(seenVersions, location.GroupKeyVersion)
		seenKeys = append(seenKeys, location.ContentGroupKey)
		seenPaths = append(seenPaths, location.ObservedRootPath)
		batch.Queue(groupLocationUpsertSQL,
			location.MediaFolderID,
			location.GroupKeyVersion,
			location.ContentGroupKey,
			location.ObservedRootPath,
			location.IsPrimary,
		)
	}
	// Match complete keys so different groups at one path remain independent.
	batch.Queue(`
		DELETE FROM media_group_locations t
		WHERE t.media_folder_id = $1
		  AND (t.observed_root_path = $2 OR t.observed_root_path LIKE $3 ESCAPE '\')
		  AND NOT EXISTS (SELECT 1 FROM unnest($4::int[], $5::text[], $6::text[]) AS s(v, k, p)
		                  WHERE s.v = t.group_key_version AND s.k = t.content_group_key AND s.p = t.observed_root_path)
	`, folderID, scopePath, scopeLike, seenVersions, seenKeys, seenPaths)
	results := r.pool.SendBatch(ctx, batch)
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("reconciling group locations in scope: %w", err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("closing group location scoped batch: %w", err)
	}
	return nil
}

// DeleteOutsideRoots removes a folder's group locations outside every configured
// root. Rows at or under any root survive even when that root is unreachable. An
// empty roots list sends no statement. It returns an error if the delete fails.
func (r *GroupLocationRepository) DeleteOutsideRoots(ctx context.Context, folderID int, roots []string) error {
	if len(roots) == 0 {
		return nil
	}
	clauses, rootArgs := pathscope.CoverageClauses("observed_root_path", roots, 2)
	args := append([]any{folderID}, rootArgs...)
	_, err := r.pool.Exec(ctx, `
		DELETE FROM media_group_locations
		WHERE media_folder_id = $1 AND NOT (`+strings.Join(clauses, " OR ")+`)
	`, args...)
	if err != nil {
		return fmt.Errorf("deleting group locations outside roots: %w", err)
	}
	return nil
}
