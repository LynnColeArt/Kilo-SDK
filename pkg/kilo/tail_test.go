package kilo

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestTailReturnsRecentAuditedMutations(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, t.TempDir(), fixedClock(time.Now()))
	defer store.Close()

	for _, id := range []string{"record:tail:1", "record:tail:2", "record:tail:3"} {
		if _, err := store.Write(ctx, Write{CreateRecord: &Record{
			ID:        id,
			Namespace: "project:kilo",
			Kind:      "tail-test",
			Summary:   "Tail entries should expose audited mutations.",
		}}); err != nil {
			t.Fatalf("write %s: %v", id, err)
		}
	}

	result, err := store.Tail(ctx, TailQuery{Limit: 2})
	if err != nil {
		t.Fatalf("tail mutations: %v", err)
	}
	if len(result.Entries) != 2 {
		t.Fatalf("expected two tail entries, got %+v", result)
	}
	if got := []uint64{result.Entries[0].Seq, result.Entries[1].Seq}; got[0] != 2 || got[1] != 3 {
		t.Fatalf("unexpected tail seqs: %v", got)
	}
	if got := recordIDsFromTail(result.Entries); strings.Join(got, ",") != "record:tail:2,record:tail:3" {
		t.Fatalf("unexpected tail records: %v", got)
	}

	fromSeq, err := store.Tail(ctx, TailQuery{MinSeq: 3})
	if err != nil {
		t.Fatalf("tail from seq: %v", err)
	}
	if len(fromSeq.Entries) != 1 || fromSeq.Entries[0].Seq != 3 {
		t.Fatalf("unexpected min seq tail: %+v", fromSeq)
	}
}

func TestTailRejectsNegativeLimitAndPropagatesContext(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, t.TempDir(), fixedClock(time.Now()))
	defer store.Close()

	if _, err := store.Tail(ctx, TailQuery{Limit: -1}); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("expected limit error, got %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Tail(cancelled, TailQuery{}); err != context.Canceled {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func recordIDsFromTail(entries []TailEntry) []string {
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		ids = append(ids, entry.Mutation.Record.ID)
	}
	return ids
}
