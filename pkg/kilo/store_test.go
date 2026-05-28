package kilo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCreateRecordSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	clock := fixedClock(time.Date(2026, 5, 9, 8, 0, 0, 0, time.UTC))

	store := openTestStore(t, path, clock)
	result, err := store.Apply(ctx, Mutation{
		Type:           MutationCreateRecord,
		IdempotencyKey: "create-pref-1",
		Actor: Actor{
			HumanID:   "human:lynn",
			SessionID: "session:1",
		},
		Record: Record{
			ID:        "record:pref-1",
			Namespace: "project:sugarfang",
			Kind:      "preference",
			Summary:   "Lynn prefers SDK-first packaging for Kilo.",
			Tags:      []string{"engineering", "packaging"},
			Attributes: map[string]string{
				"scope": "project",
			},
		},
	})
	if err != nil {
		t.Fatalf("create record: %v", err)
	}
	if result.Seq != 1 || result.Record.Revision != 1 {
		t.Fatalf("unexpected create result: %+v", result)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened := openTestStore(t, path, clock)
	defer reopened.Close()

	record, found, err := reopened.GetRecord(ctx, "record:pref-1")
	if err != nil {
		t.Fatalf("get record: %v", err)
	}
	if !found {
		t.Fatal("expected record after reopen")
	}
	if record.Summary != "Lynn prefers SDK-first packaging for Kilo." {
		t.Fatalf("unexpected summary: %q", record.Summary)
	}
	if record.Revision != 1 || record.CreatedSeq != 1 || record.UpdatedSeq != 1 {
		t.Fatalf("unexpected replayed record metadata: %+v", record)
	}
	if status := reopened.Status(); status.DurableSeq != 1 || status.Records != 1 {
		t.Fatalf("unexpected status after replay: %+v", status)
	}
}

func TestStoreSingleOwnerLockRejectsConcurrentOpen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	now := time.Date(2026, 5, 9, 8, 15, 0, 0, time.UTC)

	first := openTestStore(t, path, fixedClock(now))
	status := first.Status()
	if status.Owner.PID != os.Getpid() {
		t.Fatalf("expected current process owner, got %+v", status.Owner)
	}
	if status.Owner.Path == "" || status.Owner.LockPath != filepath.Join(status.Owner.Path, ownerLockName) || !status.Owner.OpenedAt.Equal(now) {
		t.Fatalf("unexpected owner metadata: %+v", status.Owner)
	}

	second, err := Open(ctx, Options{Path: path, SyncWrites: true, Clock: fixedClock(now)})
	if err == nil {
		_ = second.Close()
		t.Fatal("expected concurrent open to fail")
	}
	if !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("expected ErrStoreLocked, got %v", err)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}
	reopened := openTestStore(t, path, fixedClock(now.Add(time.Minute)))
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}
}

func TestStoreOwnerLockRejectsConcurrentProcess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store")

	store := openTestStore(t, path, fixedClock(time.Now()))
	defer store.Close()

	cmd := exec.Command(os.Args[0], "-test.run=TestStoreOwnerLockHelperProcess", "--", path)
	cmd.Env = append(os.Environ(), "KILO_OWNER_LOCK_CHILD=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child lock check failed: %v\n%s", err, output)
	}
}

func TestStoreOwnerLockHelperProcess(t *testing.T) {
	if os.Getenv("KILO_OWNER_LOCK_CHILD") != "1" {
		return
	}
	path := ""
	for i, arg := range os.Args {
		if arg == "--" && i+1 < len(os.Args) {
			path = os.Args[i+1]
			break
		}
	}
	if path == "" {
		t.Fatal("missing child store path")
	}

	store, err := Open(context.Background(), Options{Path: path, SyncWrites: true})
	if err == nil {
		_ = store.Close()
		t.Fatal("expected child open to fail while parent owns store")
	}
	if !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("expected ErrStoreLocked, got %v", err)
	}
}

func TestCatalogSnapshotLoadsSnapshotAndReplaysTail(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	clock := fixedClock(time.Date(2026, 5, 9, 9, 0, 0, 0, time.UTC))
	store := openTestStore(t, path, clock)

	createMutation := Mutation{
		Type:           MutationCreateRecord,
		IdempotencyKey: "snapshot-create-pref",
		Record: Record{
			ID:        "record:snapshot-pref",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Snapshots should hydrate catalog state.",
			Tags:      []string{"snapshot"},
			HumanID:   "human:lynn",
		},
	}
	created, err := store.Apply(ctx, createMutation)
	if err != nil {
		t.Fatalf("create snapshot record: %v", err)
	}
	if _, err := store.Apply(ctx, Mutation{
		Type: MutationCreateNode,
		Node: Node{
			ID:      "node:snapshot-pref",
			Type:    "memory_record",
			Summary: "Snapshot-backed memory node.",
			Attributes: map[string]string{
				"record_id": "record:snapshot-pref",
			},
		},
	}); err != nil {
		t.Fatalf("create snapshot node: %v", err)
	}

	snapshot, err := store.WriteSnapshot(ctx)
	if err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	if snapshot.DurableSeq != 2 || snapshot.Version != 1 || snapshot.Checksum == "" {
		t.Fatalf("unexpected snapshot metadata: %+v", snapshot)
	}
	if status := store.Status(); status.SnapshotSeq != 2 {
		t.Fatalf("expected in-process snapshot watermark, got %+v", status)
	}

	if _, err := store.Apply(ctx, Mutation{
		Type:             MutationUpdateRecord,
		ExpectedRevision: created.Record.Revision,
		Record: Record{
			ID:        "record:snapshot-pref",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Tail replay should update snapshot-backed catalog state.",
			Tags:      []string{"snapshot", "tail"},
			HumanID:   "human:lynn",
		},
	}); err != nil {
		t.Fatalf("update after snapshot: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened := openTestStore(t, path, clock)
	defer reopened.Close()
	status := reopened.Status()
	if status.SnapshotSeq != 2 || status.DurableSeq != 3 || status.Records != 1 || status.Nodes != 1 {
		t.Fatalf("unexpected reopened status: %+v", status)
	}
	record, found, err := reopened.GetRecord(ctx, "record:snapshot-pref")
	if err != nil {
		t.Fatalf("get snapshot-backed record: %v", err)
	}
	if !found || record.Summary != "Tail replay should update snapshot-backed catalog state." || record.UpdatedSeq != 3 {
		t.Fatalf("unexpected snapshot-backed record: found=%v record=%+v", found, record)
	}
	queried, err := reopened.QueryRecords(ctx, RecordQuery{
		Namespace: "project:kilo",
		Kind:      "preference",
		HumanID:   "human:lynn",
		Tags:      []string{"tail"},
	})
	if err != nil {
		t.Fatalf("query snapshot-backed record: %v", err)
	}
	if ids := recordIDs(queried.Records); strings.Join(ids, ",") != "record:snapshot-pref" {
		t.Fatalf("unexpected query results after snapshot load: %v", ids)
	}
	duplicate, err := reopened.Apply(ctx, createMutation)
	if err != nil {
		t.Fatalf("repeat idempotent mutation after snapshot load: %v", err)
	}
	if !duplicate.Idempotent || duplicate.Seq != created.Seq {
		t.Fatalf("expected snapshot-backed idempotency result, got %+v", duplicate)
	}
	if status := reopened.Status(); status.DurableSeq != 3 {
		t.Fatalf("idempotent replay should not append after snapshot load: %+v", status)
	}
}

func TestOpenRejectsInvalidCatalogSnapshotVersion(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))
	if _, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:snapshot-version",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Snapshot version validation matters.",
		},
	}); err != nil {
		t.Fatalf("create snapshot record: %v", err)
	}
	if _, err := store.WriteSnapshot(ctx); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	pathToSnapshot := filepath.Join(path, "snapshots", "catalog.json")
	raw, err := os.ReadFile(pathToSnapshot)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	raw = bytes.Replace(raw, []byte(`"version": 1`), []byte(`"version": 99`), 1)
	if err := os.WriteFile(pathToSnapshot, raw, 0o644); err != nil {
		t.Fatalf("corrupt snapshot version: %v", err)
	}

	_, err = Open(ctx, Options{Path: path})
	if err == nil || !strings.Contains(err.Error(), "unsupported catalog snapshot version") {
		t.Fatalf("expected unsupported snapshot version error, got %v", err)
	}
}

func TestOpenRejectsCatalogSnapshotChecksumMismatch(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))
	if _, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:snapshot-checksum",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Snapshot checksums must be validated.",
		},
	}); err != nil {
		t.Fatalf("create snapshot record: %v", err)
	}
	snapshot, err := store.WriteSnapshot(ctx)
	if err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	pathToSnapshot := filepath.Join(path, "snapshots", "catalog.json")
	raw, err := os.ReadFile(pathToSnapshot)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	raw = bytes.Replace(raw, []byte(snapshot.Checksum), []byte("sha256:bad"), 1)
	if err := os.WriteFile(pathToSnapshot, raw, 0o644); err != nil {
		t.Fatalf("corrupt snapshot checksum: %v", err)
	}

	_, err = Open(ctx, Options{Path: path})
	if err == nil || !strings.Contains(err.Error(), "catalog snapshot checksum mismatch") {
		t.Fatalf("expected snapshot checksum error, got %v", err)
	}
}

func TestCompactionWritesManifestArchivesSegmentAndPreservesQueries(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))

	created, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:compact-active",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Initial compactable preference.",
			Tags:      []string{"before"},
			HumanID:   "human:lynn",
		},
	})
	if err != nil {
		t.Fatalf("create compact active record: %v", err)
	}
	if _, err := store.Apply(ctx, Mutation{
		Type:             MutationUpdateRecord,
		ExpectedRevision: created.Record.Revision,
		Record: Record{
			ID:        "record:compact-active",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Compacted catalog keeps the latest record state.",
			Tags:      []string{"after"},
			HumanID:   "human:lynn",
		},
	}); err != nil {
		t.Fatalf("update compact active record: %v", err)
	}
	deleted, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:compact-deleted",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Deleted records stay as audited tombstones.",
		},
	})
	if err != nil {
		t.Fatalf("create compact deleted record: %v", err)
	}
	if _, err := store.Apply(ctx, Mutation{
		Type:             MutationDeleteRecord,
		ExpectedRevision: deleted.Record.Revision,
		Record:           Record{ID: "record:compact-deleted"},
	}); err != nil {
		t.Fatalf("delete compact record: %v", err)
	}
	if _, err := store.Apply(ctx, Mutation{
		Type: MutationCreateNode,
		Node: Node{ID: "node:compact-memory", Type: "memory_record", Summary: "Compacted graph node."},
	}); err != nil {
		t.Fatalf("create compact node: %v", err)
	}

	before := store.Status()
	compaction, err := store.Compact(ctx)
	if err != nil {
		t.Fatalf("compact store: %v", err)
	}
	if compaction.CompactedSeq != before.DurableSeq || compaction.Snapshot.DurableSeq != before.DurableSeq {
		t.Fatalf("unexpected compaction watermark: before=%+v compaction=%+v", before, compaction)
	}
	if compaction.RemovedFrames != int(before.DurableSeq) || compaction.KeptFrames != 0 {
		t.Fatalf("unexpected compaction frame counts: %+v", compaction)
	}
	if compaction.ArchivedSegment == "" || compaction.Path == "" || compaction.Checksum == "" {
		t.Fatalf("compaction should report archive, manifest, and checksum: %+v", compaction)
	}
	if _, err := os.Stat(compaction.ArchivedSegment); err != nil {
		t.Fatalf("stat archived segment: %v", err)
	}
	if _, err := os.Stat(compaction.Path); err != nil {
		t.Fatalf("stat compaction manifest: %v", err)
	}
	if got := segmentBaseSeq(t, path); got != before.DurableSeq+1 {
		t.Fatalf("expected compacted segment base seq %d, got %d", before.DurableSeq+1, got)
	}
	if status := store.Status(); status.CompactionSeq != before.DurableSeq || status.SnapshotSeq != before.DurableSeq {
		t.Fatalf("unexpected in-process compaction status: %+v", status)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close compacted store: %v", err)
	}
	reopened := openTestStore(t, path, fixedClock(time.Now()))
	defer reopened.Close()
	status := reopened.Status()
	if status.CompactionSeq != before.DurableSeq || status.DurableSeq != before.DurableSeq || status.Records != 1 || status.Tombstones != 1 || status.Nodes != 1 {
		t.Fatalf("unexpected reopened compacted status: %+v", status)
	}
	queried, err := reopened.QueryRecords(ctx, RecordQuery{
		Namespace: "project:kilo",
		Kind:      "preference",
		HumanID:   "human:lynn",
		Tags:      []string{"after"},
	})
	if err != nil {
		t.Fatalf("query compacted records: %v", err)
	}
	if ids := recordIDs(queried.Records); strings.Join(ids, ",") != "record:compact-active" {
		t.Fatalf("unexpected compacted query results: %v", ids)
	}
	tombstone, found, err := reopened.GetRecordIncludingDeleted(ctx, "record:compact-deleted")
	if err != nil {
		t.Fatalf("get compacted tombstone: %v", err)
	}
	if !found || !tombstone.Deleted {
		t.Fatalf("expected compacted tombstone, found=%v record=%+v", found, tombstone)
	}
	next, err := reopened.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:compact-tail",
			Namespace: "project:kilo",
			Kind:      "note",
			Summary:   "Writes continue after segment compaction.",
		},
	})
	if err != nil {
		t.Fatalf("append after compaction: %v", err)
	}
	if next.Seq != before.DurableSeq+1 {
		t.Fatalf("unexpected post-compaction sequence: %+v", next)
	}
}

func TestCompactionRejectsLaggingProjectors(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "store"), fixedClock(time.Now()))
	defer store.Close()

	projector := &testProjector{
		name:      "vector-test",
		entered:   make(chan ProjectionEvent, 1),
		release:   make(chan struct{}),
		processed: make(chan ProjectionEvent, 2),
	}
	if err := store.RegisterProjector(ctx, projector); err != nil {
		t.Fatalf("register projector: %v", err)
	}
	if _, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:lagging-compact",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Lagging projectors should block compaction.",
		},
	}); err != nil {
		t.Fatalf("create lagging compact record: %v", err)
	}
	receiveProjectionEvent(t, projector.entered)
	_, err := store.Compact(ctx)
	if err == nil || !strings.Contains(err.Error(), "projector") {
		t.Fatalf("expected projector lag compaction error, got %v", err)
	}
	close(projector.release)
	waitForProjectorSeq(t, store, "vector-test", 1)
	if _, err := store.Compact(ctx); err != nil {
		t.Fatalf("compact after projector catch-up: %v", err)
	}
}

func TestRegisterProjectorRejectsCompactedEventGap(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))
	if _, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:compacted-projector-gap",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Compacted events require explicit projector rebuild support.",
		},
	}); err != nil {
		t.Fatalf("create compacted projector gap record: %v", err)
	}
	if _, err := store.Compact(ctx); err != nil {
		t.Fatalf("compact store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close compacted store: %v", err)
	}

	reopened := openTestStore(t, path, fixedClock(time.Now()))
	defer reopened.Close()
	err := reopened.RegisterProjector(ctx, &testProjector{name: "new-vector-test"})
	if err == nil || !strings.Contains(err.Error(), "compacted projection events") {
		t.Fatalf("expected compacted projection event gap error, got %v", err)
	}
}

func TestOpenRejectsCompactionManifestChecksumMismatch(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))
	if _, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:compaction-manifest-checksum",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Compaction manifests are validated on open.",
		},
	}); err != nil {
		t.Fatalf("create compaction manifest checksum record: %v", err)
	}
	compaction, err := store.Compact(ctx)
	if err != nil {
		t.Fatalf("compact store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close compacted store: %v", err)
	}

	raw, err := os.ReadFile(compaction.Path)
	if err != nil {
		t.Fatalf("read compaction manifest: %v", err)
	}
	raw = bytes.Replace(raw, []byte(compaction.Checksum), []byte("sha256:bad"), 1)
	if err := os.WriteFile(compaction.Path, raw, 0o644); err != nil {
		t.Fatalf("corrupt compaction manifest checksum: %v", err)
	}

	_, err = Open(ctx, Options{Path: path})
	if err == nil || !strings.Contains(err.Error(), "compaction manifest checksum mismatch") {
		t.Fatalf("expected compaction manifest checksum error, got %v", err)
	}
}

func TestPurgeAndRedactionRequestsAreAuditedSeparatelyFromDelete(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))

	created, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:retention-target",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Retention operations must be explicit.",
		},
	})
	if err != nil {
		t.Fatalf("create retention target: %v", err)
	}
	if _, err := store.Apply(ctx, Mutation{
		Type:             MutationDeleteRecord,
		ExpectedRevision: created.Record.Revision,
		Record:           Record{ID: "record:retention-target"},
	}); err != nil {
		t.Fatalf("delete retention target: %v", err)
	}
	purge, err := store.Apply(ctx, Mutation{
		Type: MutationCreatePurgeRequest,
		PurgeRequest: PurgeRequest{
			ID:         "purge:retention-target",
			Action:     PurgeActionPurge,
			TargetType: "record",
			TargetID:   "record:retention-target",
			Reason:     "User requested permanent removal review.",
		},
	})
	if err != nil {
		t.Fatalf("create purge request: %v", err)
	}
	redaction, err := store.Apply(ctx, Mutation{
		Type: MutationCreatePurgeRequest,
		PurgeRequest: PurgeRequest{
			ID:         "redact:retention-target",
			Action:     PurgeActionRedact,
			TargetType: "record",
			TargetID:   "record:retention-target",
			Reason:     "User requested sensitive field review.",
		},
	})
	if err != nil {
		t.Fatalf("create redaction request: %v", err)
	}
	if purge.PurgeRequest.CreatedSeq != 3 || redaction.PurgeRequest.CreatedSeq != 4 {
		t.Fatalf("unexpected purge request sequences: purge=%+v redaction=%+v", purge.PurgeRequest, redaction.PurgeRequest)
	}
	record, found, err := store.GetRecordIncludingDeleted(ctx, "record:retention-target")
	if err != nil {
		t.Fatalf("get retention target: %v", err)
	}
	if !found || !record.Deleted {
		t.Fatalf("delete should remain an audit tombstone, found=%v record=%+v", found, record)
	}
	request, found, err := store.GetPurgeRequest(ctx, "purge:retention-target")
	if err != nil {
		t.Fatalf("get purge request: %v", err)
	}
	if !found || request.Action != PurgeActionPurge || request.TargetID != "record:retention-target" {
		t.Fatalf("unexpected purge request: found=%v request=%+v", found, request)
	}
	if status := store.Status(); status.PurgeRequests != 2 || status.Tombstones != 1 {
		t.Fatalf("unexpected retention status: %+v", status)
	}
	if _, err := store.Compact(ctx); err != nil {
		t.Fatalf("compact retention store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close retention store: %v", err)
	}

	reopened := openTestStore(t, path, fixedClock(time.Now()))
	defer reopened.Close()
	request, found, err = reopened.GetPurgeRequest(ctx, "redact:retention-target")
	if err != nil {
		t.Fatalf("get compacted redaction request: %v", err)
	}
	if !found || request.Action != PurgeActionRedact {
		t.Fatalf("unexpected compacted redaction request: found=%v request=%+v", found, request)
	}
	record, found, err = reopened.GetRecordIncludingDeleted(ctx, "record:retention-target")
	if err != nil {
		t.Fatalf("get compacted retention target: %v", err)
	}
	if !found || !record.Deleted {
		t.Fatalf("compaction should preserve tombstone until explicit purge execution exists: found=%v record=%+v", found, record)
	}
}

func TestUpdateRequiresExpectedRevision(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))

	created, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:decision-1",
			Namespace: "project:kilo",
			Kind:      "decision",
			Summary:   "Use Go for the initial implementation.",
		},
	})
	if err != nil {
		t.Fatalf("create record: %v", err)
	}

	updated, err := store.Apply(ctx, Mutation{
		Type:             MutationUpdateRecord,
		ExpectedRevision: created.Record.Revision,
		Record: Record{
			ID:        "record:decision-1",
			Namespace: "project:kilo",
			Kind:      "decision",
			Summary:   "Use Go for the whole first implementation.",
		},
	})
	if err != nil {
		t.Fatalf("update record: %v", err)
	}
	if updated.Record.Revision != 2 || updated.Seq != 2 {
		t.Fatalf("unexpected update result: %+v", updated)
	}

	_, err = store.Apply(ctx, Mutation{
		Type:             MutationUpdateRecord,
		ExpectedRevision: created.Record.Revision,
		Record: Record{
			ID:        "record:decision-1",
			Namespace: "project:kilo",
			Kind:      "decision",
			Summary:   "Stale update should not land.",
		},
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if status := store.Status(); status.DurableSeq != 2 {
		t.Fatalf("conflict should not append a mutation: %+v", status)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	reopened := openTestStore(t, path, fixedClock(time.Now()))
	defer reopened.Close()
	record, found, err := reopened.GetRecord(ctx, "record:decision-1")
	if err != nil {
		t.Fatalf("get record after replay: %v", err)
	}
	if !found || record.Summary != "Use Go for the whole first implementation." || record.Revision != 2 {
		t.Fatalf("unexpected record after replay: found=%v record=%+v", found, record)
	}
}

func TestIdempotencyKeyDeduplicatesRetryAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	mutation := Mutation{
		Type:           MutationCreateRecord,
		IdempotencyKey: "create-record:anchor-1",
		Record: Record{
			ID:        "record:anchor-1",
			Namespace: "project:kilo",
			Kind:      "anchor",
			Summary:   "Kilo canonical state is append-only.",
		},
	}

	store := openTestStore(t, path, fixedClock(time.Now()))
	first, err := store.Apply(ctx, mutation)
	if err != nil {
		t.Fatalf("first apply: %v", err)
	}
	second, err := store.Apply(ctx, mutation)
	if err != nil {
		t.Fatalf("retry apply: %v", err)
	}
	if !second.Idempotent || second.Seq != first.Seq {
		t.Fatalf("expected in-process idempotent result, first=%+v second=%+v", first, second)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened := openTestStore(t, path, fixedClock(time.Now()))
	defer reopened.Close()
	third, err := reopened.Apply(ctx, mutation)
	if err != nil {
		t.Fatalf("retry after reopen: %v", err)
	}
	if !third.Idempotent || third.Seq != first.Seq {
		t.Fatalf("expected replayed idempotent result, first=%+v third=%+v", first, third)
	}
	if status := reopened.Status(); status.DurableSeq != 1 || status.Records != 1 {
		t.Fatalf("idempotent retry should not append: %+v", status)
	}
}

func TestDeleteCreatesTombstoneByDefault(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "store"), fixedClock(time.Now()))
	defer store.Close()

	created, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:obsolete",
			Namespace: "project:kilo",
			Kind:      "context_summary",
			Summary:   "Old summary.",
		},
	})
	if err != nil {
		t.Fatalf("create record: %v", err)
	}
	deleted, err := store.Apply(ctx, Mutation{
		Type:             MutationDeleteRecord,
		ExpectedRevision: created.Record.Revision,
		Record: Record{
			ID: "record:obsolete",
		},
	})
	if err != nil {
		t.Fatalf("delete record: %v", err)
	}
	if !deleted.Record.Deleted || deleted.Record.Revision != 2 {
		t.Fatalf("expected tombstone revision: %+v", deleted.Record)
	}

	_, found, err := store.GetRecord(ctx, "record:obsolete")
	if err != nil {
		t.Fatalf("get record: %v", err)
	}
	if found {
		t.Fatal("tombstoned record should be hidden from default reads")
	}
	record, found, err := store.GetRecordIncludingDeleted(ctx, "record:obsolete")
	if err != nil {
		t.Fatalf("get deleted record: %v", err)
	}
	if !found || !record.Deleted {
		t.Fatalf("expected deleted record to be visible through audit read: found=%v record=%+v", found, record)
	}
}

func TestCorruptLogRecordFailsLoudly(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))
	_, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:corruption-check",
			Namespace: "project:kilo",
			Kind:      "decision",
			Summary:   "Corruption should be detected.",
		},
	})
	if err != nil {
		t.Fatalf("create record: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	segmentPath := filepath.Join(path, "segments", "00000000000000000001.kseg")
	file, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open segment for corruption: %v", err)
	}
	if _, err := file.WriteString(`{"data":{"version":1,"seq":2,"payload":"nope"},"crc32":"00000000"}` + "\n"); err != nil {
		t.Fatalf("append corrupt frame: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close corrupt segment: %v", err)
	}

	_, err = Open(ctx, Options{Path: path})
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("expected checksum error, got %v", err)
	}
}

func TestPartialTrailingLogFrameFailsLoudly(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))
	_, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:partial-check",
			Namespace: "project:kilo",
			Kind:      "decision",
			Summary:   "Partial trailing writes should be detected.",
		},
	})
	if err != nil {
		t.Fatalf("create record: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	segmentPath := filepath.Join(path, "segments", "00000000000000000001.kseg")
	file, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open segment for partial write: %v", err)
	}
	if _, err := file.WriteString(`{"data":`); err != nil {
		t.Fatalf("append partial frame: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close partial segment: %v", err)
	}

	_, err = Open(ctx, Options{Path: path})
	if err == nil || !strings.Contains(err.Error(), "partial") {
		t.Fatalf("expected partial frame error, got %v", err)
	}
}

func TestQueryRecordsCombinesMetadataFilters(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))

	records := []Record{
		{
			ID:        "record:lynn-pref-1",
			Namespace: "project:sugarfang",
			Kind:      "preference",
			Summary:   "Lynn prefers SDK-first storage boundaries.",
			Tags:      []string{"Engineering", "Storage"},
			HumanID:   "human:lynn",
			SessionID: "session:alpha",
			MissionID: "mission:plan",
			Persona:   "planner",
			ProjectID: "project:sugarfang",
		},
		{
			ID:        "record:lynn-pref-2",
			Namespace: "project:sugarfang",
			Kind:      "preference",
			Summary:   "Lynn wants hierarchy to be structural.",
			Tags:      []string{"engineering", "hierarchy"},
			HumanID:   "human:lynn",
			SessionID: "session:beta",
			ProjectID: "project:sugarfang",
		},
		{
			ID:        "record:other-pref",
			Namespace: "project:sugarfang",
			Kind:      "preference",
			Summary:   "Another human has a different packaging preference.",
			Tags:      []string{"engineering", "storage"},
			HumanID:   "human:alex",
			SessionID: "session:alpha",
			ProjectID: "project:sugarfang",
		},
		{
			ID:        "record:lynn-decision",
			Namespace: "project:sugarfang",
			Kind:      "decision",
			Summary:   "Kilo starts in Go.",
			Tags:      []string{"engineering", "storage"},
			HumanID:   "human:lynn",
			SessionID: "session:alpha",
			ProjectID: "project:sugarfang",
		},
	}
	for _, record := range records {
		if _, err := store.Apply(ctx, Mutation{Type: MutationCreateRecord, Record: record}); err != nil {
			t.Fatalf("create %s: %v", record.ID, err)
		}
	}

	result, err := store.QueryRecords(ctx, RecordQuery{
		Namespace: "project:sugarfang",
		Kind:      "preference",
		HumanID:   "human:lynn",
		SessionID: "session:alpha",
		Tags:      []string{"storage"},
	})
	if err != nil {
		t.Fatalf("query records: %v", err)
	}
	if len(result.Records) != 1 || result.Records[0].ID != "record:lynn-pref-1" {
		t.Fatalf("unexpected filtered result: %+v", result)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	reopened := openTestStore(t, path, fixedClock(time.Now()))
	defer reopened.Close()
	replayed, err := reopened.QueryRecords(ctx, RecordQuery{
		Namespace: "project:sugarfang",
		Kind:      "preference",
		HumanID:   "human:lynn",
		Tags:      []string{"engineering"},
	})
	if err != nil {
		t.Fatalf("query replayed records: %v", err)
	}
	if ids := recordIDs(replayed.Records); strings.Join(ids, ",") != "record:lynn-pref-2,record:lynn-pref-1" {
		t.Fatalf("unexpected replayed order/results: %v", ids)
	}
}

func TestQueryRecordsHidesTombstonesUnlessRequested(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "store"), fixedClock(time.Now()))
	defer store.Close()

	created, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:deleted-pref",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "This preference was superseded.",
			Tags:      []string{"engineering"},
			SessionID: "session:alpha",
		},
	})
	if err != nil {
		t.Fatalf("create record: %v", err)
	}
	if _, err := store.Apply(ctx, Mutation{
		Type:             MutationDeleteRecord,
		ExpectedRevision: created.Record.Revision,
		Record:           Record{ID: created.Record.ID},
	}); err != nil {
		t.Fatalf("delete record: %v", err)
	}

	visible, err := store.QueryRecords(ctx, RecordQuery{
		Namespace: "project:kilo",
		Kind:      "preference",
	})
	if err != nil {
		t.Fatalf("query visible records: %v", err)
	}
	if len(visible.Records) != 0 {
		t.Fatalf("tombstone should be hidden by default: %+v", visible.Records)
	}

	withDeleted, err := store.QueryRecords(ctx, RecordQuery{
		Namespace:       "project:kilo",
		Kind:            "preference",
		IncludeDeleted:  true,
		IncludeAuditIDs: true,
	})
	if err != nil {
		t.Fatalf("query deleted records: %v", err)
	}
	if len(withDeleted.Records) != 1 || !withDeleted.Records[0].Deleted || len(withDeleted.CandidateIDs) != 1 {
		t.Fatalf("expected deleted record and audit candidate id: %+v", withDeleted)
	}
}

func TestCreateChunkStoresMetadataSeparatelyAndContentSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))

	content := []byte("planner: Kilo should keep transcript bodies out of memory metadata.")
	created, err := store.CreateChunk(ctx, Chunk{
		ID:        "chunk:session-alpha:000001",
		Namespace: "project:kilo",
		ProjectID: "project:kilo",
		SessionID: "session:alpha",
		Kind:      "transcript",
		Sequence:  1,
		Attributes: map[string]string{
			"speaker": "planner",
		},
	}, content)
	if err != nil {
		t.Fatalf("create chunk: %v", err)
	}
	if created.Seq != 1 || created.Chunk.Revision != 1 {
		t.Fatalf("unexpected create result: %+v", created)
	}
	if created.Chunk.ContentLength != int64(len(content)) {
		t.Fatalf("unexpected content length: %+v", created.Chunk)
	}
	if !strings.HasPrefix(created.Chunk.ContentHash, "sha256:") {
		t.Fatalf("expected sha256 content hash: %+v", created.Chunk)
	}

	metadata, found, err := store.GetChunk(ctx, "chunk:session-alpha:000001")
	if err != nil {
		t.Fatalf("get chunk metadata: %v", err)
	}
	if !found || metadata.ContentLength != int64(len(content)) || metadata.Attributes["speaker"] != "planner" {
		t.Fatalf("unexpected chunk metadata: found=%v chunk=%+v", found, metadata)
	}
	body, found, err := store.ReadChunkContent(ctx, "chunk:session-alpha:000001")
	if err != nil {
		t.Fatalf("read chunk body: %v", err)
	}
	if !found || !bytes.Equal(body, content) {
		t.Fatalf("unexpected chunk body: found=%v body=%q", found, string(body))
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	reopened := openTestStore(t, path, fixedClock(time.Now()))
	defer reopened.Close()

	replayed, found, err := reopened.GetChunk(ctx, "chunk:session-alpha:000001")
	if err != nil {
		t.Fatalf("get replayed chunk metadata: %v", err)
	}
	if !found || replayed.ContentHash != created.Chunk.ContentHash || replayed.ContentLength != created.Chunk.ContentLength {
		t.Fatalf("unexpected replayed metadata: found=%v chunk=%+v", found, replayed)
	}
	replayedBody, found, err := reopened.ReadChunkContent(ctx, "chunk:session-alpha:000001")
	if err != nil {
		t.Fatalf("read replayed chunk body: %v", err)
	}
	if !found || !bytes.Equal(replayedBody, content) {
		t.Fatalf("unexpected replayed body: found=%v body=%q", found, string(replayedBody))
	}
	if status := reopened.Status(); status.DurableSeq != 1 || status.Chunks != 1 {
		t.Fatalf("unexpected status after replay: %+v", status)
	}
}

func TestQueryChunksReturnsOrderedMetadataWithoutBodies(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "store"), fixedClock(time.Now()))
	defer store.Close()

	for _, item := range []struct {
		id        string
		sessionID string
		sequence  uint64
		body      string
	}{
		{id: "chunk:alpha:000002", sessionID: "session:alpha", sequence: 2, body: "second alpha chunk"},
		{id: "chunk:beta:000001", sessionID: "session:beta", sequence: 1, body: "first beta chunk"},
		{id: "chunk:alpha:000001", sessionID: "session:alpha", sequence: 1, body: "first alpha chunk"},
	} {
		_, err := store.CreateChunk(ctx, Chunk{
			ID:        item.id,
			Namespace: "project:kilo",
			ProjectID: "project:kilo",
			SessionID: item.sessionID,
			Kind:      "transcript",
			Sequence:  item.sequence,
		}, []byte(item.body))
		if err != nil {
			t.Fatalf("create chunk %s: %v", item.id, err)
		}
	}

	result, err := store.QueryChunks(ctx, ChunkQuery{
		Namespace:         "project:kilo",
		SessionID:         "session:alpha",
		Kind:              "transcript",
		IncludeCandidates: true,
	})
	if err != nil {
		t.Fatalf("query chunks: %v", err)
	}
	if ids := chunkIDs(result.Chunks); strings.Join(ids, ",") != "chunk:alpha:000001,chunk:alpha:000002" {
		t.Fatalf("unexpected chunk order: %v", ids)
	}
	if strings.Join(result.CandidateIDs, ",") != "chunk:alpha:000001,chunk:alpha:000002" {
		t.Fatalf("unexpected candidate ids: %v", result.CandidateIDs)
	}
	for _, chunk := range result.Chunks {
		if chunk.ContentHash == "" || chunk.ContentLength == 0 {
			t.Fatalf("expected metadata without body reads: %+v", chunk)
		}
	}
}

func TestReadChunkContentDetectsCorruption(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "store"), fixedClock(time.Now()))
	defer store.Close()

	const id = "chunk:corruption-check"
	if _, err := store.CreateChunk(ctx, Chunk{
		ID:        id,
		Namespace: "project:kilo",
		Kind:      "transcript",
		Sequence:  1,
	}, []byte("original body")); err != nil {
		t.Fatalf("create chunk: %v", err)
	}
	if err := os.WriteFile(store.chunkBodies.path(id), []byte("tampered body"), 0o644); err != nil {
		t.Fatalf("tamper chunk body: %v", err)
	}
	_, found, err := store.ReadChunkContent(ctx, id)
	if err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("expected body verification mismatch, found=%v err=%v", found, err)
	}
}

func TestSourceSpanLetsMemoryRecordCiteOrderedChunksAndSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))

	if _, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:pref-sdk",
			Namespace: "project:kilo",
			ProjectID: "project:kilo",
			Kind:      "preference",
			Summary:   "Lynn prefers SDK-first packaging.",
			SessionID: "session:alpha",
		},
	}); err != nil {
		t.Fatalf("create memory record: %v", err)
	}
	for _, item := range []struct {
		id       string
		sequence uint64
		body     string
	}{
		{id: "chunk:alpha:000002", sequence: 2, body: "and should remain embeddable as an SDK."},
		{id: "chunk:alpha:000001", sequence: 1, body: "Kilo should start as an embedded library"},
	} {
		if _, err := store.CreateChunk(ctx, Chunk{
			ID:        item.id,
			Namespace: "project:kilo",
			ProjectID: "project:kilo",
			SessionID: "session:alpha",
			Kind:      "transcript",
			Sequence:  item.sequence,
		}, []byte(item.body)); err != nil {
			t.Fatalf("create chunk %s: %v", item.id, err)
		}
	}

	created, err := store.CreateSourceSpan(ctx, SourceSpan{
		ID:        "span:pref-sdk-source",
		Namespace: "project:kilo",
		ProjectID: "project:kilo",
		SessionID: "session:alpha",
		RecordID:  "record:pref-sdk",
		ChunkRefs: []SourceSpanChunkRef{
			{ChunkID: "chunk:alpha:000002", EndOffset: 38},
			{ChunkID: "chunk:alpha:000001", StartOffset: 0, EndOffset: 40},
		},
	})
	if err != nil {
		t.Fatalf("create source span: %v", err)
	}
	if created.Seq != 4 || created.SourceSpan.Revision != 1 {
		t.Fatalf("unexpected source span result: %+v", created)
	}

	read, found, err := store.ReadSourceSpan(ctx, "span:pref-sdk-source")
	if err != nil {
		t.Fatalf("read source span: %v", err)
	}
	if !found || read.Span.RecordID != "record:pref-sdk" {
		t.Fatalf("unexpected source span read: found=%v read=%+v", found, read)
	}
	if ids := chunkIDs(read.Chunks); strings.Join(ids, ",") != "chunk:alpha:000001,chunk:alpha:000002" {
		t.Fatalf("expected ordered chunk references, got %v", ids)
	}
	if refs := chunkRefIDs(read.Span.ChunkRefs); strings.Join(refs, ",") != "chunk:alpha:000001,chunk:alpha:000002" {
		t.Fatalf("expected ordered span refs, got %v", refs)
	}

	byRecord, err := store.QuerySourceSpans(ctx, SourceSpanQuery{
		RecordID:          "record:pref-sdk",
		IncludeCandidates: true,
	})
	if err != nil {
		t.Fatalf("query source spans: %v", err)
	}
	if len(byRecord.Spans) != 1 || byRecord.Spans[0].ID != "span:pref-sdk-source" {
		t.Fatalf("unexpected source span query result: %+v", byRecord)
	}
	if strings.Join(byRecord.CandidateIDs, ",") != "span:pref-sdk-source" {
		t.Fatalf("unexpected candidate ids: %v", byRecord.CandidateIDs)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	reopened := openTestStore(t, path, fixedClock(time.Now()))
	defer reopened.Close()
	replayed, found, err := reopened.ReadSourceSpan(ctx, "span:pref-sdk-source")
	if err != nil {
		t.Fatalf("read replayed source span: %v", err)
	}
	if !found || strings.Join(chunkIDs(replayed.Chunks), ",") != "chunk:alpha:000001,chunk:alpha:000002" {
		t.Fatalf("unexpected replayed source span: found=%v read=%+v", found, replayed)
	}
	if status := reopened.Status(); status.DurableSeq != 4 || status.SourceSpans != 1 {
		t.Fatalf("unexpected status after replay: %+v", status)
	}
}

func TestCreateSourceSpanRejectsMissingChunkReference(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "store"), fixedClock(time.Now()))
	defer store.Close()

	if _, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:pref",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Preference with missing source.",
		},
	}); err != nil {
		t.Fatalf("create memory record: %v", err)
	}
	_, err := store.CreateSourceSpan(ctx, SourceSpan{
		ID:        "span:missing-chunk",
		Namespace: "project:kilo",
		RecordID:  "record:pref",
		ChunkRefs: []SourceSpanChunkRef{{ChunkID: "chunk:missing"}},
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected missing chunk not found, got %v", err)
	}
	if status := store.Status(); status.DurableSeq != 1 || status.SourceSpans != 0 {
		t.Fatalf("missing chunk span should not append: %+v", status)
	}
}

func TestGraphTraversalSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))

	for _, node := range []Node{
		{ID: "node:human:lynn", Type: "human", Name: "Lynn"},
		{ID: "node:project:sugarfang", Type: "project", Name: "SugarFang"},
		{ID: "node:session:alpha", Type: "session", Name: "Session Alpha"},
		{ID: "node:mission:planner", Type: "mission", Name: "Planner mission"},
	} {
		if _, err := store.Apply(ctx, Mutation{Type: MutationCreateNode, Node: node}); err != nil {
			t.Fatalf("create node %s: %v", node.ID, err)
		}
	}
	for _, edge := range []Edge{
		{ID: "edge:lynn-project", Type: "participates_in", FromNodeID: "node:human:lynn", ToNodeID: "node:project:sugarfang"},
		{ID: "edge:project-session", Type: "contains", FromNodeID: "node:project:sugarfang", ToNodeID: "node:session:alpha"},
		{ID: "edge:session-mission", Type: "contains", FromNodeID: "node:session:alpha", ToNodeID: "node:mission:planner"},
	} {
		if _, err := store.Apply(ctx, Mutation{Type: MutationCreateEdge, Edge: edge}); err != nil {
			t.Fatalf("create edge %s: %v", edge.ID, err)
		}
	}

	children, err := store.Children(ctx, "node:project:sugarfang", EdgeQuery{Type: "contains"})
	if err != nil {
		t.Fatalf("children: %v", err)
	}
	if nodeIDs(children.Nodes)[0] != "node:session:alpha" {
		t.Fatalf("unexpected children: %+v", children.Nodes)
	}
	parents, err := store.Parents(ctx, "node:mission:planner", EdgeQuery{Type: "contains"})
	if err != nil {
		t.Fatalf("parents: %v", err)
	}
	if nodeIDs(parents.Nodes)[0] != "node:session:alpha" {
		t.Fatalf("unexpected parents: %+v", parents.Nodes)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened := openTestStore(t, path, fixedClock(time.Now()))
	defer reopened.Close()
	replayed, err := reopened.Children(ctx, "node:project:sugarfang", EdgeQuery{Type: "contains"})
	if err != nil {
		t.Fatalf("children after replay: %v", err)
	}
	if nodeIDs(replayed.Nodes)[0] != "node:session:alpha" || replayed.Edges[0].ID != "edge:project-session" {
		t.Fatalf("unexpected replayed traversal: %+v", replayed)
	}
}

func TestEdgeTombstoneRemovesTraversalButPreservesAuditRead(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "store"), fixedClock(time.Now()))
	defer store.Close()

	for _, node := range []Node{
		{ID: "node:project", Type: "project", Name: "Project"},
		{ID: "node:session", Type: "session", Name: "Session"},
	} {
		if _, err := store.Apply(ctx, Mutation{Type: MutationCreateNode, Node: node}); err != nil {
			t.Fatalf("create node %s: %v", node.ID, err)
		}
	}
	created, err := store.Apply(ctx, Mutation{
		Type: MutationCreateEdge,
		Edge: Edge{ID: "edge:contains", Type: "contains", FromNodeID: "node:project", ToNodeID: "node:session"},
	})
	if err != nil {
		t.Fatalf("create edge: %v", err)
	}
	if _, err := store.Apply(ctx, Mutation{
		Type:             MutationDeleteEdge,
		ExpectedRevision: created.Edge.Revision,
		Edge:             Edge{ID: "edge:contains"},
	}); err != nil {
		t.Fatalf("delete edge: %v", err)
	}

	children, err := store.Children(ctx, "node:project", EdgeQuery{Type: "contains"})
	if err != nil {
		t.Fatalf("children: %v", err)
	}
	if len(children.Nodes) != 0 || len(children.Edges) != 0 {
		t.Fatalf("deleted edge should not traverse: %+v", children)
	}
	_, found, err := store.GetEdge(ctx, "edge:contains")
	if err != nil {
		t.Fatalf("get edge: %v", err)
	}
	if found {
		t.Fatal("deleted edge should be hidden from default edge read")
	}
	deleted, found, err := store.GetEdgeIncludingDeleted(ctx, "edge:contains")
	if err != nil {
		t.Fatalf("get deleted edge: %v", err)
	}
	if !found || !deleted.Deleted || deleted.Revision != 2 {
		t.Fatalf("expected audit-visible edge tombstone: found=%v edge=%+v", found, deleted)
	}
}

func TestNodeUpdateAndDeleteUseRevisions(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "store"), fixedClock(time.Now()))
	defer store.Close()

	created, err := store.Apply(ctx, Mutation{
		Type: MutationCreateNode,
		Node: Node{ID: "node:human:lynn", Type: "human", Name: "Lynn", Summary: "Initial summary."},
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	updated, err := store.Apply(ctx, Mutation{
		Type:             MutationUpdateNode,
		ExpectedRevision: created.Node.Revision,
		Node: Node{
			ID:      "node:human:lynn",
			Type:    "human",
			Name:    "Lynn",
			Summary: "Engineering preference owner.",
		},
	})
	if err != nil {
		t.Fatalf("update node: %v", err)
	}
	if updated.Node.Revision != 2 || updated.Node.Summary != "Engineering preference owner." {
		t.Fatalf("unexpected updated node: %+v", updated.Node)
	}
	_, err = store.Apply(ctx, Mutation{
		Type:             MutationUpdateNode,
		ExpectedRevision: created.Node.Revision,
		Node: Node{
			ID:      "node:human:lynn",
			Type:    "human",
			Name:    "Lynn",
			Summary: "Stale update.",
		},
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected node conflict, got %v", err)
	}
	deleted, err := store.Apply(ctx, Mutation{
		Type:             MutationDeleteNode,
		ExpectedRevision: updated.Node.Revision,
		Node:             Node{ID: "node:human:lynn"},
	})
	if err != nil {
		t.Fatalf("delete node: %v", err)
	}
	if !deleted.Node.Deleted || deleted.Node.Revision != 3 {
		t.Fatalf("unexpected deleted node: %+v", deleted.Node)
	}
	_, found, err := store.GetNode(ctx, "node:human:lynn")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if found {
		t.Fatal("deleted node should be hidden by default")
	}
	auditNode, found, err := store.GetNodeIncludingDeleted(ctx, "node:human:lynn")
	if err != nil {
		t.Fatalf("get deleted node: %v", err)
	}
	if !found || !auditNode.Deleted {
		t.Fatalf("expected deleted node through audit read: found=%v node=%+v", found, auditNode)
	}
}

func TestQueryNodesFindsConversationEpisodesForHumanAndExpandsContext(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))

	for _, node := range []Node{
		{ID: "node:human:lynn", Type: "human", Name: "Lynn"},
		{ID: "node:human:alex", Type: "human", Name: "Alex"},
		{ID: "node:project:kilo", Type: "project", Name: "Kilo"},
		{ID: "node:session:alpha", Type: "session", Name: "Storage architecture session"},
		{ID: "node:session:beta", Type: "session", Name: "Unrelated session"},
		{ID: "node:episode:preferences", Type: "episode", Name: "Engineering preferences"},
		{ID: "node:memory:pref", Type: "memory_record", Name: "SDK-first preference"},
	} {
		if _, err := store.Apply(ctx, Mutation{Type: MutationCreateNode, Node: node}); err != nil {
			t.Fatalf("create node %s: %v", node.ID, err)
		}
	}
	for _, edge := range []Edge{
		{ID: "edge:project-alpha", Type: "contains", FromNodeID: "node:project:kilo", ToNodeID: "node:session:alpha"},
		{ID: "edge:project-beta", Type: "contains", FromNodeID: "node:project:kilo", ToNodeID: "node:session:beta"},
		{ID: "edge:lynn-alpha", Type: "participates_in", FromNodeID: "node:human:lynn", ToNodeID: "node:session:alpha"},
		{ID: "edge:alex-beta", Type: "participates_in", FromNodeID: "node:human:alex", ToNodeID: "node:session:beta"},
		{ID: "edge:alpha-episode", Type: "contains", FromNodeID: "node:session:alpha", ToNodeID: "node:episode:preferences"},
		{ID: "edge:pref-evidence", Type: "evidence_for", FromNodeID: "node:memory:pref", ToNodeID: "node:session:alpha"},
	} {
		if _, err := store.Apply(ctx, Mutation{Type: MutationCreateEdge, Edge: edge}); err != nil {
			t.Fatalf("create edge %s: %v", edge.ID, err)
		}
	}

	result, err := store.QueryNodes(ctx, NodeQuery{
		Type:              "session",
		RelatedNodeID:     "node:human:lynn",
		RelatedEdgeType:   "participates_in",
		RelatedDirection:  EdgeDirectionOutgoing,
		ExpandParents:     true,
		ParentEdgeType:    "contains",
		ExpandChildren:    true,
		ChildEdgeType:     "contains",
		ExpandEvidence:    true,
		EvidenceEdgeType:  "evidence_for",
		IncludeDeleted:    false,
		IncludeCandidates: true,
	})
	if err != nil {
		t.Fatalf("query nodes: %v", err)
	}
	if ids := nodeIDs(result.Nodes); strings.Join(ids, ",") != "node:session:alpha" {
		t.Fatalf("unexpected session nodes: %v", ids)
	}
	if ids := edgeIDs(result.MatchEdges); strings.Join(ids, ",") != "edge:lynn-alpha" {
		t.Fatalf("unexpected match edges: %v", ids)
	}
	if ids := nodeIDs(result.Expanded.Nodes); strings.Join(ids, ",") != "node:episode:preferences,node:memory:pref,node:project:kilo" {
		t.Fatalf("unexpected expanded nodes: %v", ids)
	}
	if ids := edgeIDs(result.Expanded.Edges); strings.Join(ids, ",") != "edge:alpha-episode,edge:pref-evidence,edge:project-alpha" {
		t.Fatalf("unexpected expanded edges: %v", ids)
	}
	if strings.Join(result.CandidateIDs, ",") != "node:session:alpha" {
		t.Fatalf("unexpected candidates: %v", result.CandidateIDs)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	reopened := openTestStore(t, path, fixedClock(time.Now()))
	defer reopened.Close()
	replayed, err := reopened.QueryNodes(ctx, NodeQuery{
		Type:             "session",
		RelatedNodeID:    "node:human:lynn",
		RelatedEdgeType:  "participates_in",
		RelatedDirection: EdgeDirectionOutgoing,
	})
	if err != nil {
		t.Fatalf("query replayed nodes: %v", err)
	}
	if ids := nodeIDs(replayed.Nodes); strings.Join(ids, ",") != "node:session:alpha" {
		t.Fatalf("unexpected replayed sessions: %v", ids)
	}
}

func TestQueryNodesExpandsSourceSpansFromEvidenceMemoryNodes(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))

	if _, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:pref-sdk",
			Namespace: "project:kilo",
			ProjectID: "project:kilo",
			Kind:      "preference",
			Summary:   "Lynn prefers SDK-first packaging.",
			SessionID: "session:alpha",
		},
	}); err != nil {
		t.Fatalf("create memory record: %v", err)
	}
	for _, item := range []struct {
		id       string
		sequence uint64
		body     string
	}{
		{id: "chunk:alpha:000002", sequence: 2, body: "and source spans should cite the transcript."},
		{id: "chunk:alpha:000001", sequence: 1, body: "Planner: keep Kilo SDK-first"},
	} {
		if _, err := store.CreateChunk(ctx, Chunk{
			ID:        item.id,
			Namespace: "project:kilo",
			ProjectID: "project:kilo",
			SessionID: "session:alpha",
			Kind:      "transcript",
			Sequence:  item.sequence,
		}, []byte(item.body)); err != nil {
			t.Fatalf("create chunk %s: %v", item.id, err)
		}
	}
	if _, err := store.CreateSourceSpan(ctx, SourceSpan{
		ID:        "span:pref-sdk-source",
		Namespace: "project:kilo",
		ProjectID: "project:kilo",
		SessionID: "session:alpha",
		RecordID:  "record:pref-sdk",
		ChunkRefs: []SourceSpanChunkRef{
			{ChunkID: "chunk:alpha:000002"},
			{ChunkID: "chunk:alpha:000001"},
		},
	}); err != nil {
		t.Fatalf("create source span: %v", err)
	}
	for _, node := range []Node{
		{ID: "node:human:lynn", Type: "human", Name: "Lynn"},
		{ID: "node:session:alpha", Type: "session", Name: "Storage architecture session"},
		{
			ID:   "node:memory:pref",
			Type: "memory_record",
			Name: "SDK-first preference",
			Attributes: map[string]string{
				"record_id": "record:pref-sdk",
			},
		},
		{
			ID:   "node:memory:unlinked",
			Type: "memory_record",
			Name: "No record bridge",
		},
	} {
		if _, err := store.Apply(ctx, Mutation{Type: MutationCreateNode, Node: node}); err != nil {
			t.Fatalf("create node %s: %v", node.ID, err)
		}
	}
	for _, edge := range []Edge{
		{ID: "edge:lynn-alpha", Type: "participates_in", FromNodeID: "node:human:lynn", ToNodeID: "node:session:alpha"},
		{ID: "edge:pref-evidence", Type: "evidence_for", FromNodeID: "node:memory:pref", ToNodeID: "node:session:alpha"},
		{ID: "edge:unlinked-evidence", Type: "evidence_for", FromNodeID: "node:memory:unlinked", ToNodeID: "node:session:alpha"},
	} {
		if _, err := store.Apply(ctx, Mutation{Type: MutationCreateEdge, Edge: edge}); err != nil {
			t.Fatalf("create edge %s: %v", edge.ID, err)
		}
	}

	result, err := store.QueryNodes(ctx, NodeQuery{
		Type:              "session",
		RelatedNodeID:     "node:human:lynn",
		RelatedEdgeType:   "participates_in",
		RelatedDirection:  EdgeDirectionOutgoing,
		ExpandEvidence:    true,
		ExpandSourceSpans: true,
	})
	if err != nil {
		t.Fatalf("query nodes: %v", err)
	}
	if ids := nodeIDs(result.Expanded.Nodes); strings.Join(ids, ",") != "node:memory:pref,node:memory:unlinked" {
		t.Fatalf("unexpected evidence expansion: %v", ids)
	}
	if len(result.SourceSpans) != 1 {
		t.Fatalf("expected one explicit source span expansion: %+v", result.SourceSpans)
	}
	if result.SourceSpans[0].Span.ID != "span:pref-sdk-source" {
		t.Fatalf("unexpected source span: %+v", result.SourceSpans[0].Span)
	}
	if ids := chunkIDs(result.SourceSpans[0].Chunks); strings.Join(ids, ",") != "chunk:alpha:000001,chunk:alpha:000002" {
		t.Fatalf("unexpected source span chunks: %v", ids)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	reopened := openTestStore(t, path, fixedClock(time.Now()))
	defer reopened.Close()
	replayed, err := reopened.QueryNodes(ctx, NodeQuery{
		Type:              "session",
		RelatedNodeID:     "node:human:lynn",
		RelatedEdgeType:   "participates_in",
		RelatedDirection:  EdgeDirectionOutgoing,
		ExpandEvidence:    true,
		ExpandSourceSpans: true,
	})
	if err != nil {
		t.Fatalf("query replayed nodes: %v", err)
	}
	if len(replayed.SourceSpans) != 1 || strings.Join(chunkIDs(replayed.SourceSpans[0].Chunks), ",") != "chunk:alpha:000001,chunk:alpha:000002" {
		t.Fatalf("unexpected replayed source spans: %+v", replayed.SourceSpans)
	}
}

func TestQueryNodesSupportsAttributeAndTimeFilters(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "store"), fixedClock(time.Now()))
	defer store.Close()

	early := time.Date(2026, 5, 9, 8, 0, 0, 0, time.UTC)
	late := early.Add(time.Hour)
	for _, mutation := range []Mutation{
		{
			Type: MutationCreateNode,
			At:   early,
			Node: Node{
				ID:      "node:episode:old",
				Type:    "episode",
				Name:    "Old preference episode",
				Summary: "Older note.",
				Attributes: map[string]string{
					"topic": "engineering_preferences",
				},
			},
		},
		{
			Type: MutationCreateNode,
			At:   late,
			Node: Node{
				ID:      "node:episode:new",
				Type:    "episode",
				Name:    "New preference episode",
				Summary: "Newer note.",
				Attributes: map[string]string{
					"topic": "engineering_preferences",
				},
			},
		},
		{
			Type: MutationCreateNode,
			At:   late,
			Node: Node{
				ID:   "node:episode:other",
				Type: "episode",
				Name: "Other episode",
				Attributes: map[string]string{
					"topic": "release",
				},
			},
		},
	} {
		if _, err := store.Apply(ctx, mutation); err != nil {
			t.Fatalf("create node %s: %v", mutation.Node.ID, err)
		}
	}

	result, err := store.QueryNodes(ctx, NodeQuery{
		Type: "episode",
		Attributes: map[string]string{
			"topic": "engineering_preferences",
		},
		UpdatedAfter: early.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("query nodes: %v", err)
	}
	if ids := nodeIDs(result.Nodes); strings.Join(ids, ",") != "node:episode:new" {
		t.Fatalf("unexpected filtered nodes: %v", ids)
	}
}

func TestProjectorRuntimeReportsLagAndCatchesUp(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "store"), fixedClock(time.Now()))
	defer store.Close()

	projector := &testProjector{
		name:      "vector-test",
		entered:   make(chan ProjectionEvent, 1),
		release:   make(chan struct{}),
		processed: make(chan ProjectionEvent, 4),
	}
	if err := store.RegisterProjector(ctx, projector); err != nil {
		t.Fatalf("register projector: %v", err)
	}

	if _, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:lag-1",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "First projector event.",
		},
	}); err != nil {
		t.Fatalf("create first record: %v", err)
	}
	first := receiveProjectionEvent(t, projector.entered)
	if first.Seq != 1 {
		t.Fatalf("expected first projector event seq 1, got %+v", first)
	}

	if _, err := store.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:lag-2",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Second projector event.",
		},
	}); err != nil {
		t.Fatalf("create second record: %v", err)
	}

	lagging := store.Status().Projectors["vector-test"]
	if lagging.IndexedSeq != 0 || lagging.DurableSeq != 2 || lagging.Lag != 2 || !lagging.Running {
		t.Fatalf("expected visible projector lag, got %+v", lagging)
	}

	close(projector.release)
	waitForProjectorSeq(t, store, "vector-test", 2)
	status := store.Status().Projectors["vector-test"]
	if status.Lag != 0 || status.IndexedSeq != 2 {
		t.Fatalf("expected projector catch-up, got %+v", status)
	}
}

func TestProjectorRuntimeResumesFromDurableCheckpointAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store")
	store := openTestStore(t, path, fixedClock(time.Now()))

	firstProjector := &testProjector{
		name:      "vector-test",
		processed: make(chan ProjectionEvent, 8),
	}
	if err := store.RegisterProjector(ctx, firstProjector); err != nil {
		t.Fatalf("register first projector: %v", err)
	}
	for _, id := range []string{"record:resume-1", "record:resume-2"} {
		if _, err := store.Apply(ctx, Mutation{
			Type: MutationCreateRecord,
			Record: Record{
				ID:        id,
				Namespace: "project:kilo",
				Kind:      "preference",
				Summary:   "Checkpointed projector event.",
			},
		}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	waitForProjectorSeq(t, store, "vector-test", 2)
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened := openTestStore(t, path, fixedClock(time.Now()))
	defer reopened.Close()
	secondProjector := &testProjector{
		name:      "vector-test",
		processed: make(chan ProjectionEvent, 8),
	}
	if err := reopened.RegisterProjector(ctx, secondProjector); err != nil {
		t.Fatalf("register second projector: %v", err)
	}
	waitForProjectorSeq(t, reopened, "vector-test", 2)
	assertNoProjectionEvent(t, secondProjector.processed)

	if _, err := reopened.Apply(ctx, Mutation{
		Type: MutationCreateRecord,
		Record: Record{
			ID:        "record:resume-3",
			Namespace: "project:kilo",
			Kind:      "preference",
			Summary:   "Only this event should project after reopen.",
		},
	}); err != nil {
		t.Fatalf("create third record: %v", err)
	}
	next := receiveProjectionEvent(t, secondProjector.processed)
	if next.Seq != 3 {
		t.Fatalf("projector should resume at seq 3, got %+v", next)
	}
	waitForProjectorSeq(t, reopened, "vector-test", 3)
	status := reopened.Status().Projectors["vector-test"]
	if status.Lag != 0 || status.IndexedSeq != 3 || status.DurableSeq != 3 {
		t.Fatalf("unexpected resumed projector status: %+v", status)
	}
}

func TestWaitForProjectionTimesOutWithStaleCoverageWithoutDroppingWrites(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "store"), fixedClock(time.Now()))
	defer store.Close()

	projector := &testProjector{
		name:      "vector-test",
		entered:   make(chan ProjectionEvent, 1),
		release:   make(chan struct{}),
		processed: make(chan ProjectionEvent, 4),
	}
	if err := store.RegisterProjector(ctx, projector); err != nil {
		t.Fatalf("register projector: %v", err)
	}
	for _, id := range []string{"record:wait-1", "record:wait-2"} {
		if _, err := store.Apply(ctx, Mutation{
			Type: MutationCreateRecord,
			Record: Record{
				ID:        id,
				Namespace: "project:kilo",
				Kind:      "preference",
				Summary:   "Durable write while projector is behind.",
			},
		}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	first := receiveProjectionEvent(t, projector.entered)
	if first.Seq != 1 {
		t.Fatalf("expected blocked first projector event, got %+v", first)
	}

	stale, err := store.WaitForProjection(ctx, ProjectionWait{
		Projector:     "vector-test",
		MinIndexedSeq: 2,
		Timeout:       20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("wait for projection: %v", err)
	}
	if !stale.TimedOut || !stale.Stale || stale.Satisfied {
		t.Fatalf("expected stale timeout, got %+v", stale)
	}
	if stale.Projector.IndexedSeq != 0 || stale.Projector.DurableSeq != 2 || stale.Projector.Lag != 2 {
		t.Fatalf("unexpected stale projector status: %+v", stale.Projector)
	}
	for _, id := range []string{"record:wait-1", "record:wait-2"} {
		if _, found, err := store.GetRecord(ctx, id); err != nil || !found {
			t.Fatalf("committed write should remain durable id=%s found=%v err=%v", id, found, err)
		}
	}

	close(projector.release)
	fresh, err := store.WaitForProjection(ctx, ProjectionWait{
		Projector:     "vector-test",
		MinIndexedSeq: 2,
		Timeout:       time.Second,
	})
	if err != nil {
		t.Fatalf("wait for catch-up: %v", err)
	}
	if !fresh.Satisfied || fresh.Stale || fresh.TimedOut || fresh.Projector.IndexedSeq != 2 {
		t.Fatalf("expected fresh catch-up, got %+v", fresh)
	}
}

func TestWaitForProjectionCanEnforceLagThreshold(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "store"), fixedClock(time.Now()))
	defer store.Close()

	projector := &testProjector{
		name:      "vector-test",
		blockSeq:  2,
		entered:   make(chan ProjectionEvent, 2),
		release:   make(chan struct{}),
		processed: make(chan ProjectionEvent, 4),
	}
	if err := store.RegisterProjector(ctx, projector); err != nil {
		t.Fatalf("register projector: %v", err)
	}
	for _, id := range []string{"record:threshold-1", "record:threshold-2"} {
		if _, err := store.Apply(ctx, Mutation{
			Type: MutationCreateRecord,
			Record: Record{
				ID:        id,
				Namespace: "project:kilo",
				Kind:      "preference",
				Summary:   "Lag-threshold projector event.",
			},
		}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	waitForProjectorSeq(t, store, "vector-test", 1)
	first := receiveProjectionEvent(t, projector.entered)
	if first.Seq != 1 {
		t.Fatalf("expected first projector event, got %+v", first)
	}
	second := receiveProjectionEvent(t, projector.entered)
	if second.Seq != 2 {
		t.Fatalf("expected blocked second projector event, got %+v", second)
	}

	stale, err := store.WaitForProjection(ctx, ProjectionWait{
		Projector:     "vector-test",
		MinIndexedSeq: 1,
		MaxLag:        0,
		EnforceMaxLag: true,
		Timeout:       20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("wait for lag threshold: %v", err)
	}
	if !stale.TimedOut || !stale.Stale || stale.Projector.IndexedSeq != 1 || stale.Projector.Lag != 1 {
		t.Fatalf("expected stale threshold timeout, got %+v", stale)
	}

	close(projector.release)
	fresh, err := store.WaitForProjection(ctx, ProjectionWait{
		Projector:     "vector-test",
		MinIndexedSeq: 1,
		MaxLag:        0,
		EnforceMaxLag: true,
		Timeout:       time.Second,
	})
	if err != nil {
		t.Fatalf("wait for threshold catch-up: %v", err)
	}
	if !fresh.Satisfied || fresh.Stale || fresh.TimedOut || fresh.Projector.Lag != 0 {
		t.Fatalf("expected fresh threshold catch-up, got %+v", fresh)
	}
}

func openTestStore(t *testing.T, path string, clock Clock) *Store {
	t.Helper()
	store, err := Open(context.Background(), Options{
		Path:       path,
		SyncWrites: true,
		Clock:      clock,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}

type fixedClock time.Time

func (c fixedClock) Now() time.Time {
	return time.Time(c)
}

func recordIDs(records []Record) []string {
	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.ID)
	}
	return ids
}

func nodeIDs(nodes []Node) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	return ids
}

func edgeIDs(edges []Edge) []string {
	ids := make([]string, 0, len(edges))
	for _, edge := range edges {
		ids = append(ids, edge.ID)
	}
	return ids
}

func chunkIDs(chunks []Chunk) []string {
	ids := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		ids = append(ids, chunk.ID)
	}
	return ids
}

func chunkRefIDs(refs []SourceSpanChunkRef) []string {
	ids := make([]string, 0, len(refs))
	for _, ref := range refs {
		ids = append(ids, ref.ChunkID)
	}
	return ids
}

func segmentBaseSeq(t *testing.T, storePath string) uint64 {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(storePath, "segments", "00000000000000000001.kseg"))
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	line, _, found := strings.Cut(string(raw), "\n")
	if !found {
		t.Fatalf("segment header missing newline: %q", string(raw))
	}
	var header struct {
		BaseSeq uint64 `json:"base_seq"`
	}
	if err := json.Unmarshal([]byte(line), &header); err != nil {
		t.Fatalf("decode segment header: %v", err)
	}
	return header.BaseSeq
}

type testProjector struct {
	name      string
	blockSeq  uint64
	entered   chan ProjectionEvent
	release   chan struct{}
	processed chan ProjectionEvent
	once      sync.Once
}

func (p *testProjector) Name() string {
	return p.name
}

func (p *testProjector) Project(ctx context.Context, event ProjectionEvent) error {
	if p.entered != nil {
		select {
		case p.entered <- event:
		default:
		}
	}
	if p.release != nil && (p.blockSeq == 0 || p.blockSeq == event.Seq) {
		var err error
		p.once.Do(func() {
			select {
			case <-ctx.Done():
				err = ctx.Err()
			case <-p.release:
			}
		})
		if err != nil {
			return err
		}
	}
	if p.processed != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case p.processed <- event:
		}
	}
	return nil
}

func receiveProjectionEvent(t *testing.T, events <-chan ProjectionEvent) ProjectionEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for projection event")
	}
	return ProjectionEvent{}
}

func assertNoProjectionEvent(t *testing.T, events <-chan ProjectionEvent) {
	t.Helper()
	select {
	case event := <-events:
		t.Fatalf("unexpected projection event: %+v", event)
	case <-time.After(50 * time.Millisecond):
	}
}

func waitForProjectorSeq(t *testing.T, store *Store, name string, seq uint64) ProjectorStatus {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		status, ok := store.Status().Projectors[name]
		if ok && status.IndexedSeq >= seq {
			return status
		}
		time.Sleep(5 * time.Millisecond)
	}
	status := store.Status().Projectors[name]
	t.Fatalf("timed out waiting for projector %q to reach seq %d; status=%+v", name, seq, status)
	return ProjectorStatus{}
}
