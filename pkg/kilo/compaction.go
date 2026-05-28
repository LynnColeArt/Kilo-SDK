package kilo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	compactionManifestType    = "kilo.compaction_manifest"
	compactionManifestVersion = 1
	compactionSegmentName     = "00000000000000000001.kseg"
)

type compactionManifestEnvelope struct {
	Type     string                 `json:"type"`
	Version  int                    `json:"version"`
	Data     compactionManifestData `json:"data"`
	Checksum string                 `json:"checksum"`
}

type compactionManifestChecksumPayload struct {
	Type    string                 `json:"type"`
	Version int                    `json:"version"`
	Data    compactionManifestData `json:"data"`
}

type compactionManifestData struct {
	CompactedSeq    uint64           `json:"compacted_seq"`
	Snapshot        SnapshotMetadata `json:"snapshot"`
	Counts          CatalogCounts    `json:"counts"`
	RemovedFrames   int              `json:"removed_frames"`
	KeptFrames      int              `json:"kept_frames"`
	ActiveSegment   string           `json:"active_segment"`
	ArchivedSegment string           `json:"archived_segment"`
	CreatedAt       time.Time        `json:"created_at"`
}

func (s *Store) Compact(ctx context.Context) (CompactionMetadata, error) {
	if err := ctx.Err(); err != nil {
		return CompactionMetadata{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return CompactionMetadata{}, fmt.Errorf("store is closed")
	}
	if s.durableSeq == 0 {
		return CompactionMetadata{}, fmt.Errorf("no durable mutations to compact")
	}
	if err := s.ensureProjectorsReadyForCompactionLocked(s.durableSeq); err != nil {
		return CompactionMetadata{}, err
	}

	createdAt := s.clock.Now().UTC()
	data := s.catalogSnapshotDataLocked(createdAt)
	snapshot, err := writeCatalogSnapshotData(s.path, data, s.syncWrites)
	if err != nil {
		return CompactionMetadata{}, err
	}
	archivePath := compactionArchivePath(s.path, data.DurableSeq)
	stats, err := s.log.CompactThrough(data.DurableSeq, archivePath)
	if err != nil {
		return CompactionMetadata{}, err
	}
	metadata, err := writeCompactionManifest(s.path, compactionManifestData{
		CompactedSeq:    data.DurableSeq,
		Snapshot:        snapshot,
		Counts:          s.catalogCountsLocked(),
		RemovedFrames:   stats.RemovedFrames,
		KeptFrames:      stats.KeptFrames,
		ActiveSegment:   stats.ActiveSegmentPath,
		ArchivedSegment: stats.ArchivedSegmentPath,
		CreatedAt:       createdAt,
	}, s.syncWrites)
	if err != nil {
		return CompactionMetadata{}, err
	}
	s.snapshotSeq = data.DurableSeq
	s.compactionSeq = data.DurableSeq
	s.projectionEvents = projectionEventsAfterSeq(s.projectionEvents, data.DurableSeq)
	return metadata, nil
}

func (s *Store) ensureProjectorsReadyForCompactionLocked(seq uint64) error {
	for name, checkpoint := range s.projectorCheckpoints {
		if checkpoint.IndexedSeq < seq {
			return fmt.Errorf("projector %q indexed seq %d is behind compaction seq %d", name, checkpoint.IndexedSeq, seq)
		}
	}
	return nil
}

func (s *Store) catalogCountsLocked() CatalogCounts {
	counts := CatalogCounts{
		Chunks:        len(s.chunks),
		SourceSpans:   len(s.sourceSpans),
		PurgeRequests: len(s.purgeRequests),
	}
	for _, record := range s.records {
		if record.Deleted {
			counts.Tombstones++
		} else {
			counts.Records++
		}
	}
	for _, node := range s.nodes {
		if !node.Deleted {
			counts.Nodes++
		}
	}
	for _, edge := range s.edges {
		if !edge.Deleted {
			counts.Edges++
		}
	}
	return counts
}

func projectionEventsAfterSeq(events []ProjectionEvent, seq uint64) []ProjectionEvent {
	out := make([]ProjectionEvent, 0, len(events))
	for _, event := range events {
		if event.Seq > seq {
			out = append(out, cloneProjectionEvent(event))
		}
	}
	return out
}

func writeCompactionManifest(root string, data compactionManifestData, syncWrites bool) (CompactionMetadata, error) {
	envelope := compactionManifestEnvelope{
		Type:    compactionManifestType,
		Version: compactionManifestVersion,
		Data:    data,
	}
	checksum, err := compactionManifestChecksum(envelope)
	if err != nil {
		return CompactionMetadata{}, err
	}
	envelope.Checksum = checksum
	encoded, err := json.MarshalIndent(envelope, "", "  ")
	if err != nil {
		return CompactionMetadata{}, fmt.Errorf("encode compaction manifest: %w", err)
	}
	encoded = append(encoded, '\n')

	path := compactionManifestPath(root, data.CompactedSeq)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return CompactionMetadata{}, fmt.Errorf("create compaction manifest directory: %w", err)
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".manifest-*.json")
	if err != nil {
		return CompactionMetadata{}, fmt.Errorf("create compaction manifest temp file: %w", err)
	}
	tmpPath := file.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := writeAll(file, encoded); err != nil {
		_ = file.Close()
		return CompactionMetadata{}, fmt.Errorf("write compaction manifest: %w", err)
	}
	if syncWrites {
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return CompactionMetadata{}, fmt.Errorf("sync compaction manifest: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return CompactionMetadata{}, fmt.Errorf("close compaction manifest: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return CompactionMetadata{}, fmt.Errorf("install compaction manifest: %w", err)
	}
	removeTemp = false
	if syncWrites {
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			return CompactionMetadata{}, err
		}
	}
	return compactionMetadataFromEnvelope(envelope, path), nil
}

func loadLatestCompactionManifest(root string) (CompactionMetadata, bool, error) {
	paths, err := filepath.Glob(filepath.Join(compactionRoot(root), "*", "manifest.json"))
	if err != nil {
		return CompactionMetadata{}, false, fmt.Errorf("list compaction manifests: %w", err)
	}
	if len(paths) == 0 {
		return CompactionMetadata{}, false, nil
	}
	sort.Strings(paths)
	var latest CompactionMetadata
	found := false
	for _, path := range paths {
		metadata, err := loadCompactionManifest(path)
		if err != nil {
			return CompactionMetadata{}, false, err
		}
		if !found || metadata.CompactedSeq > latest.CompactedSeq {
			latest = metadata
			found = true
		}
	}
	return latest, found, nil
}

func loadCompactionManifest(path string) (CompactionMetadata, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return CompactionMetadata{}, fmt.Errorf("read compaction manifest: %w", err)
	}
	var envelope compactionManifestEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return CompactionMetadata{}, fmt.Errorf("decode compaction manifest: %w", err)
	}
	if envelope.Type != compactionManifestType {
		return CompactionMetadata{}, fmt.Errorf("unsupported compaction manifest type %q", envelope.Type)
	}
	if envelope.Version != compactionManifestVersion {
		return CompactionMetadata{}, fmt.Errorf("unsupported compaction manifest version %d", envelope.Version)
	}
	checksum, err := compactionManifestChecksum(envelope)
	if err != nil {
		return CompactionMetadata{}, err
	}
	if envelope.Checksum != checksum {
		return CompactionMetadata{}, fmt.Errorf("%w: compaction manifest checksum mismatch: expected %s got %s", ErrCorrupt, envelope.Checksum, checksum)
	}
	return compactionMetadataFromEnvelope(envelope, path), nil
}

func compactionManifestChecksum(envelope compactionManifestEnvelope) (string, error) {
	payload := compactionManifestChecksumPayload{
		Type:    envelope.Type,
		Version: envelope.Version,
		Data:    envelope.Data,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("checksum compaction manifest: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func compactionMetadataFromEnvelope(envelope compactionManifestEnvelope, path string) CompactionMetadata {
	return CompactionMetadata{
		Version:         envelope.Version,
		CompactedSeq:    envelope.Data.CompactedSeq,
		Snapshot:        envelope.Data.Snapshot,
		Counts:          envelope.Data.Counts,
		RemovedFrames:   envelope.Data.RemovedFrames,
		KeptFrames:      envelope.Data.KeptFrames,
		ActiveSegment:   envelope.Data.ActiveSegment,
		ArchivedSegment: envelope.Data.ArchivedSegment,
		Path:            path,
		Checksum:        envelope.Checksum,
		CreatedAt:       envelope.Data.CreatedAt,
	}
}

func compactionRoot(root string) string {
	return filepath.Join(root, "compactions")
}

func compactionRunDir(root string, seq uint64) string {
	return filepath.Join(compactionRoot(root), fmt.Sprintf("%020d", seq))
}

func compactionManifestPath(root string, seq uint64) string {
	return filepath.Join(compactionRunDir(root, seq), "manifest.json")
}

func compactionArchivePath(root string, seq uint64) string {
	return filepath.Join(compactionRunDir(root, seq), "segments", compactionSegmentName)
}
