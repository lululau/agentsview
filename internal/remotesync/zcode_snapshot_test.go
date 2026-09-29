package remotesync

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/parser"
)

// zcodeRoot lays out the configured-root shape: a parent directory whose
// database lives in the db/ subdirectory, matching the provider's
// normalizeZCodeRoot mapping.
func zcodeRoot(t *testing.T) (root string, dbDir string, dbPath string) {
	t.Helper()
	root = t.TempDir()
	dbDir = filepath.Join(root, "db")
	require.NoError(t, os.MkdirAll(dbDir, 0o755))
	return root, dbDir, filepath.Join(dbDir, parser.ZCodeDBName)
}

func TestResolveTargetsZCodeAdvertisesSnapshotRoot(t *testing.T) {
	root, dbDir, dbPath := zcodeRoot(t)
	require.NoError(t, os.WriteFile(dbPath, []byte("db"), 0o644))
	// A stray top-level db.sqlite must never win over the db/ layout.
	stray := filepath.Join(root, parser.ZCodeDBName)
	require.NoError(t, os.WriteFile(stray, []byte("stray"), 0o644))

	// The two default dirs (".zcode/cli/db" and ".zcode/cli") normalize to
	// the same database directory; advertise it once.
	targets := resolveTargetsForTest(t, config.Config{AgentDirs: map[parser.AgentType][]string{
		parser.AgentZCode: {root, dbDir},
	}})
	assert.Equal(t, []string{dbDir}, targets.Dirs[parser.AgentZCode])
	assert.Equal(t, []string{dbPath}, targets.Files[parser.AgentZCode])
	assert.NotContains(t, targets.AllExtraFiles(), stray)
	assert.False(t, targets.HasSanitizedFileScopedAgents(),
		"ZCode rides the manifest/delta path as a snapshot agent")
}

func TestZCodeActiveWALIsOneStableStandaloneDatabase(t *testing.T) {
	root, dbDir, dbPath := zcodeRoot(t)
	writer, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })
	_, err = writer.ExecContext(t.Context(), `PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE session (id TEXT PRIMARY KEY); INSERT INTO session VALUES ('wal-session');`)
	require.NoError(t, err)
	walInfo, err := os.Stat(dbPath + "-wal")
	require.NoError(t, err)
	assert.Greater(t, walInfo.Size(), int64(32))

	targets := resolveTargetsForTest(t, config.Config{AgentDirs: map[parser.AgentType][]string{
		parser.AgentZCode: {root},
	}})
	require.Equal(t, []string{dbDir}, targets.Dirs[parser.AgentZCode])
	require.Equal(t, []string{dbPath}, targets.Files[parser.AgentZCode])

	manifest, err := BuildManifest(t.Context(), targets)
	require.NoError(t, err)
	require.Len(t, manifest.Files, 1)
	manifestEntry := manifest.Files[0]

	var archive bytes.Buffer
	require.NoError(t, WriteArchive(t.Context(), &archive, targets))
	entries := archiveEntries(t, archive.Bytes())
	require.Len(t, entries, 1)
	var snapshot []byte
	for name, body := range entries {
		assert.NotContains(t, name, "-wal")
		if strings.HasSuffix(name, "/db/db.sqlite") {
			snapshot = body
		}
	}
	require.NotEmpty(t, snapshot, "archive keys: %v", keys(entries))

	snapshotPath := filepath.Join(t.TempDir(), parser.ZCodeDBName)
	require.NoError(t, os.WriteFile(snapshotPath, snapshot, 0o600))
	reader, err := sql.Open("sqlite3", snapshotPath)
	require.NoError(t, err)
	var id string
	require.NoError(t, reader.QueryRowContext(t.Context(), "SELECT id FROM session").Scan(&id))
	assert.Equal(t, "wal-session", id)
	require.NoError(t, reader.Close())

	for _, entry := range manifest.Files {
		assert.Equal(t, manifestEntry.Size, entry.Size)
		assert.Equal(t, manifestEntry.MtimeNS, entry.MtimeNS)
	}
}

func TestZCodeVanishedDatabaseRemainsEvictable(t *testing.T) {
	root, dbDir, dbPath := zcodeRoot(t)
	require.NoError(t, os.WriteFile(dbPath, []byte("selected"), 0o644))
	configured := config.Config{AgentDirs: map[parser.AgentType][]string{
		parser.AgentZCode: {root},
	}}
	initial := resolveTargetsForTest(t, configured)
	require.Equal(t, []string{dbPath}, initial.Files[parser.AgentZCode])
	require.NoError(t, os.Remove(dbPath))

	fresh := resolveTargetsForTest(t, configured)
	assert.Equal(t, []string{dbDir}, fresh.Dirs[parser.AgentZCode])
	assert.Equal(t, []string{dbPath}, fresh.Files[parser.AgentZCode])
	manifest, err := BuildManifest(t.Context(), fresh)
	require.NoError(t, err)
	assert.Empty(t, manifest.Files)

	files, ok := SelectAllowedFiles(fresh, []string{dbPath})
	require.True(t, ok)
	var delta bytes.Buffer
	require.NoError(t, WriteArchiveFiles(t.Context(), &delta, fresh, files))
	assert.Empty(t, archiveEntries(t, delta.Bytes()))

	mirror := t.TempDir()
	mirrorPath, err := safeRemappedRemotePath(mirror, dbPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(mirrorPath), 0o755))
	require.NoError(t, os.WriteFile(mirrorPath, []byte("stale"), 0o644))
	diff, err := MirrorDiff(mirror, manifest)
	require.NoError(t, err)
	assert.Equal(t, []string{mirrorPath}, diff.Deletions)
}

// A corrupt ZCode database degrades to a missing manifest entry so the
// archive's already-imported sessions survive and every other agent keeps
// syncing.
func TestZCodeCorruptDatabaseDoesNotBlockOtherAgents(t *testing.T) {
	root, dbDir, dbPath := zcodeRoot(t)
	require.NoError(t, os.WriteFile(dbPath, []byte("corrupt sqlite database"), 0o644))
	cursorRoot := filepath.Join(root, "cursor")
	cursorFile := filepath.Join(cursorRoot, "project", "agent-transcripts",
		"01234567-89ab-cdef-0123-456789abcdef.jsonl")
	require.NoError(t, os.MkdirAll(filepath.Dir(cursorFile), 0o755))
	require.NoError(t, os.WriteFile(cursorFile, []byte("cursor transcript"), 0o644))
	targets := TargetSet{
		Dirs: map[parser.AgentType][]string{
			parser.AgentZCode:  {dbDir},
			parser.AgentCursor: {cursorRoot},
		},
		Files: map[parser.AgentType][]string{
			parser.AgentZCode:  {dbPath},
			parser.AgentCursor: {cursorFile},
		},
	}
	manifest, err := BuildManifest(t.Context(), targets)
	require.NoError(t, err)
	assert.Equal(t, []string{cursorFile}, manifestPaths(manifest))

	var archive bytes.Buffer
	require.NoError(t, WriteArchive(t.Context(), &archive, targets))
	entries := archiveEntries(t, archive.Bytes())
	require.Len(t, entries, 1)
	assert.NotContains(t, strings.Join(keys(entries), "\n"), parser.ZCodeDBName)
}

func TestZCodeSnapshotDeltaUsesOnlineBackup(t *testing.T) {
	root, _, dbPath := zcodeRoot(t)
	writer, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })
	_, err = writer.ExecContext(t.Context(), `PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE session (id TEXT PRIMARY KEY); INSERT INTO session VALUES ('delta-session')`)
	require.NoError(t, err)
	targets := resolveTargetsForTest(t, config.Config{AgentDirs: map[parser.AgentType][]string{
		parser.AgentZCode: {root},
	}})
	files, ok := SelectAllowedFiles(targets, []string{dbPath})
	require.True(t, ok, "ZCode snapshot must be authorized as a delta file")
	var delta bytes.Buffer
	require.NoError(t, WriteArchiveFiles(t.Context(), &delta, targets, files))
	entries := archiveEntries(t, delta.Bytes())
	require.Len(t, entries, 1)
	var snapshot []byte
	for name, body := range entries {
		if strings.HasSuffix(name, "/db/db.sqlite") {
			snapshot = body
		}
	}
	require.NotEmpty(t, snapshot)
	snapshotPath := filepath.Join(t.TempDir(), parser.ZCodeDBName)
	require.NoError(t, os.WriteFile(snapshotPath, snapshot, 0o600))
	reader, err := sql.Open("sqlite3", snapshotPath)
	require.NoError(t, err)
	var id string
	require.NoError(t, reader.QueryRowContext(t.Context(), "SELECT id FROM session").Scan(&id))
	assert.Equal(t, "delta-session", id)
	require.NoError(t, reader.Close())
	require.Len(t, files, 1)
}

func TestSqliteSnapshotForArchivePathZCode(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(root, "db", parser.ZCodeDBName)
	tests := []struct {
		name   string
		path   string
		want   string
		wantOK bool
	}{
		{"db", dbPath, dbPath, true},
		{"wal", dbPath + "-wal", dbPath, true},
		{"shm", dbPath + "-shm", dbPath, true},
		{"journal", dbPath + "-journal", dbPath, true},
		{"other", filepath.Join(root, "db", "notes.sqlite"), "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := sqliteSnapshotForArchivePath(tt.path)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}
