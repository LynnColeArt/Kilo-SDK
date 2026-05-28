package segmentlog

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	segmentVersion = 1
	frameVersion   = 1
	firstBaseSeq   = 1
	segmentName    = "00000000000000000001.kseg"
)

type Log struct {
	path       string
	file       *os.File
	nextSeq    uint64
	syncWrites bool
}

type Entry struct {
	Seq     uint64
	Payload []byte
}

type CompactionStats struct {
	CompactedSeq        uint64
	RemovedFrames       int
	KeptFrames          int
	BaseSeq             uint64
	NextSeq             uint64
	ActiveSegmentPath   string
	ArchivedSegmentPath string
}

type segmentHeader struct {
	Type    string `json:"type"`
	Version int    `json:"version"`
	BaseSeq uint64 `json:"base_seq"`
}

type frameData struct {
	Version int             `json:"version"`
	Seq     uint64          `json:"seq"`
	Payload json.RawMessage `json:"payload"`
}

type frame struct {
	Data  frameData `json:"data"`
	CRC32 string    `json:"crc32"`
}

func Open(root string, syncWrites bool) (*Log, []Entry, error) {
	if root == "" {
		return nil, nil, fmt.Errorf("log root is required")
	}
	segmentsDir := filepath.Join(root, "segments")
	if err := os.MkdirAll(segmentsDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create segment directory: %w", err)
	}
	segmentPath := filepath.Join(segmentsDir, segmentName)
	if _, err := os.Stat(segmentPath); errors.Is(err, os.ErrNotExist) {
		if err := createSegment(segmentPath, firstBaseSeq, syncWrites); err != nil {
			return nil, nil, err
		}
	} else if err != nil {
		return nil, nil, fmt.Errorf("stat segment: %w", err)
	}

	entries, nextSeq, err := replay(segmentPath)
	if err != nil {
		return nil, nil, err
	}
	file, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open segment for append: %w", err)
	}
	return &Log{
		path:       segmentPath,
		file:       file,
		nextSeq:    nextSeq,
		syncWrites: syncWrites,
	}, entries, nil
}

func (l *Log) Append(seq uint64, payload []byte) error {
	if l == nil || l.file == nil {
		return fmt.Errorf("segment log is closed")
	}
	if seq != l.nextSeq {
		return fmt.Errorf("append sequence mismatch: expected %d got %d", l.nextSeq, seq)
	}
	if !json.Valid(payload) {
		return fmt.Errorf("append payload must be valid json")
	}
	data := frameData{
		Version: frameVersion,
		Seq:     seq,
		Payload: append(json.RawMessage{}, payload...),
	}
	wrapped := frame{
		Data:  data,
		CRC32: checksum(data),
	}
	encoded, err := json.Marshal(wrapped)
	if err != nil {
		return fmt.Errorf("encode segment frame: %w", err)
	}
	if err := writeFull(l.file, append(encoded, '\n')); err != nil {
		return fmt.Errorf("write segment frame: %w", err)
	}
	if l.syncWrites {
		if err := l.file.Sync(); err != nil {
			return fmt.Errorf("sync segment frame: %w", err)
		}
	}
	l.nextSeq++
	return nil
}

func (l *Log) CompactThrough(compactedSeq uint64, archivePath string) (CompactionStats, error) {
	if l == nil || l.file == nil {
		return CompactionStats{}, fmt.Errorf("segment log is closed")
	}
	if compactedSeq == 0 {
		return CompactionStats{}, fmt.Errorf("compaction sequence is required")
	}
	if compactedSeq >= l.nextSeq {
		return CompactionStats{}, fmt.Errorf("compaction sequence %d is ahead of log next sequence %d", compactedSeq, l.nextSeq)
	}
	if l.syncWrites {
		if err := l.file.Sync(); err != nil {
			return CompactionStats{}, fmt.Errorf("sync segment before compaction: %w", err)
		}
	}
	entries, _, err := replay(l.path)
	if err != nil {
		return CompactionStats{}, err
	}
	tail := make([]Entry, 0, len(entries))
	removed := 0
	for _, entry := range entries {
		if entry.Seq <= compactedSeq {
			removed++
			continue
		}
		tail = append(tail, entry)
	}
	if removed == 0 {
		return CompactionStats{}, fmt.Errorf("no segment frames at or before compaction sequence %d", compactedSeq)
	}
	if archivePath != "" {
		if err := copySegment(l.path, archivePath, l.syncWrites); err != nil {
			return CompactionStats{}, err
		}
	}

	baseSeq := compactedSeq + 1
	tmpPath := l.path + ".compact-tmp"
	if err := writeSegment(tmpPath, baseSeq, tail, l.syncWrites); err != nil {
		return CompactionStats{}, err
	}
	if err := l.file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return CompactionStats{}, fmt.Errorf("close segment before compaction install: %w", err)
	}
	l.file = nil
	if err := os.Rename(tmpPath, l.path); err != nil {
		return CompactionStats{}, fmt.Errorf("install compacted segment: %w", err)
	}
	if l.syncWrites {
		if err := syncDir(filepath.Dir(l.path)); err != nil {
			return CompactionStats{}, err
		}
	}
	file, err := os.OpenFile(l.path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return CompactionStats{}, fmt.Errorf("reopen compacted segment: %w", err)
	}
	l.file = file
	l.nextSeq = nextSeq(baseSeq, tail)
	return CompactionStats{
		CompactedSeq:        compactedSeq,
		RemovedFrames:       removed,
		KeptFrames:          len(tail),
		BaseSeq:             baseSeq,
		NextSeq:             l.nextSeq,
		ActiveSegmentPath:   l.path,
		ArchivedSegmentPath: archivePath,
	}, nil
}

func (l *Log) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

func createSegment(path string, baseSeq uint64, syncWrites bool) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create segment: %w", err)
	}
	if err := writeSegmentContent(file, baseSeq, nil); err != nil {
		_ = file.Close()
		return err
	}
	if syncWrites {
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return fmt.Errorf("sync segment header: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close segment header: %w", err)
	}
	return nil
}

func replay(path string) ([]Entry, uint64, error) {
	if err := requireTrailingNewline(path); err != nil {
		return nil, 0, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("open segment for replay: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 64*1024*1024)
	line := 0
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, 0, fmt.Errorf("read segment header: %w", err)
		}
		return nil, 0, fmt.Errorf("segment header is missing")
	}
	line++
	var header segmentHeader
	if err := json.Unmarshal(scanner.Bytes(), &header); err != nil {
		return nil, 0, fmt.Errorf("decode segment header: %w", err)
	}
	if header.Type != "kilo.segment" || header.Version != segmentVersion || header.BaseSeq == 0 {
		return nil, 0, fmt.Errorf("unsupported segment header: %+v", header)
	}

	expectedSeq := header.BaseSeq
	var entries []Entry
	for scanner.Scan() {
		line++
		raw := append([]byte{}, scanner.Bytes()...)
		var wrapped frame
		if err := json.Unmarshal(raw, &wrapped); err != nil {
			return nil, 0, fmt.Errorf("decode segment frame at line %d: %w", line, err)
		}
		if wrapped.Data.Version != frameVersion {
			return nil, 0, fmt.Errorf("unsupported frame version %d at line %d", wrapped.Data.Version, line)
		}
		if wrapped.Data.Seq != expectedSeq {
			return nil, 0, fmt.Errorf("segment sequence gap at line %d: expected %d got %d", line, expectedSeq, wrapped.Data.Seq)
		}
		if got := checksum(wrapped.Data); got != wrapped.CRC32 {
			return nil, 0, fmt.Errorf("segment checksum mismatch at seq %d: expected %s got %s", wrapped.Data.Seq, wrapped.CRC32, got)
		}
		if !json.Valid(wrapped.Data.Payload) {
			return nil, 0, fmt.Errorf("segment payload at seq %d is not valid json", wrapped.Data.Seq)
		}
		entries = append(entries, Entry{
			Seq:     wrapped.Data.Seq,
			Payload: append([]byte{}, wrapped.Data.Payload...),
		})
		expectedSeq++
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, fmt.Errorf("scan segment: %w", err)
	}
	return entries, expectedSeq, nil
}

func requireTrailingNewline(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open segment for newline check: %w", err)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat segment for newline check: %w", err)
	}
	if stat.Size() == 0 {
		return fmt.Errorf("segment is empty")
	}
	if _, err := file.Seek(-1, 2); err != nil {
		return fmt.Errorf("seek segment tail: %w", err)
	}
	var tail [1]byte
	if _, err := file.Read(tail[:]); err != nil {
		return fmt.Errorf("read segment tail: %w", err)
	}
	if tail[0] != '\n' {
		return fmt.Errorf("partial segment frame without trailing newline")
	}
	return nil
}

func checksum(data frameData) string {
	encoded, err := json.Marshal(data)
	if err != nil {
		panic(fmt.Sprintf("marshal checksum frame: %v", err))
	}
	return fmt.Sprintf("%08x", crc32.ChecksumIEEE(encoded))
}

func writeFull(file *os.File, payload []byte) error {
	for len(payload) > 0 {
		n, err := file.Write(payload)
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("short write")
		}
		payload = payload[n:]
	}
	return nil
}

func writeSegment(path string, baseSeq uint64, entries []Entry, syncWrites bool) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create compacted segment: %w", err)
	}
	if err := writeSegmentContent(file, baseSeq, entries); err != nil {
		_ = file.Close()
		return err
	}
	if syncWrites {
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return fmt.Errorf("sync compacted segment: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close compacted segment: %w", err)
	}
	return nil
}

func writeSegmentContent(file *os.File, baseSeq uint64, entries []Entry) error {
	if baseSeq == 0 {
		return fmt.Errorf("segment base sequence is required")
	}
	header := segmentHeader{
		Type:    "kilo.segment",
		Version: segmentVersion,
		BaseSeq: baseSeq,
	}
	encoded, err := json.Marshal(header)
	if err != nil {
		return fmt.Errorf("encode segment header: %w", err)
	}
	if err := writeFull(file, append(encoded, '\n')); err != nil {
		return fmt.Errorf("write segment header: %w", err)
	}
	expectedSeq := baseSeq
	for _, entry := range entries {
		if entry.Seq != expectedSeq {
			return fmt.Errorf("segment sequence gap while writing: expected %d got %d", expectedSeq, entry.Seq)
		}
		if !json.Valid(entry.Payload) {
			return fmt.Errorf("segment payload at seq %d is not valid json", entry.Seq)
		}
		data := frameData{
			Version: frameVersion,
			Seq:     entry.Seq,
			Payload: append(json.RawMessage{}, entry.Payload...),
		}
		wrapped := frame{
			Data:  data,
			CRC32: checksum(data),
		}
		encoded, err := json.Marshal(wrapped)
		if err != nil {
			return fmt.Errorf("encode segment frame: %w", err)
		}
		if err := writeFull(file, append(encoded, '\n')); err != nil {
			return fmt.Errorf("write segment frame: %w", err)
		}
		expectedSeq++
	}
	return nil
}

func nextSeq(baseSeq uint64, entries []Entry) uint64 {
	if len(entries) == 0 {
		return baseSeq
	}
	return entries[len(entries)-1].Seq + 1
}

func copySegment(sourcePath, targetPath string, syncWrites bool) error {
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return fmt.Errorf("create segment archive directory: %w", err)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open segment for archive: %w", err)
	}
	defer source.Close()
	target, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create segment archive: %w", err)
	}
	if _, err := io.Copy(target, source); err != nil {
		_ = target.Close()
		return fmt.Errorf("copy segment archive: %w", err)
	}
	if syncWrites {
		if err := target.Sync(); err != nil {
			_ = target.Close()
			return fmt.Errorf("sync segment archive: %w", err)
		}
	}
	if err := target.Close(); err != nil {
		return fmt.Errorf("close segment archive: %w", err)
	}
	if syncWrites {
		if err := syncDir(filepath.Dir(targetPath)); err != nil {
			return err
		}
	}
	return nil
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory for sync: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync directory: %w", err)
	}
	return nil
}
