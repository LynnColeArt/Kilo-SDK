package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/LynnColeArt/Kilo-SDK/internal/httpapi"
	"github.com/LynnColeArt/Kilo-SDK/internal/pilotimport"
	"github.com/LynnColeArt/Kilo-SDK/internal/pilotvalidate"
	"github.com/LynnColeArt/Kilo-SDK/pkg/kilo"
)

type options struct {
	path             string
	addr             string
	pretty           bool
	includeDeleted   bool
	syncWrites       bool
	limit            int
	minSeq           uint64
	input            string
	cases            string
	namespace        string
	projectID        string
	humanID          string
	initStore        bool
	requireFresh     bool
	parallelSessions int
	repeats          int
}

func Run(args []string, stdout, stderr io.Writer) int {
	positional, opts, err := parseArgs(args)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if len(positional) == 0 {
		usage(stderr)
		return 2
	}

	switch positional[0] {
	case "status":
		return runStatus(positional[1:], opts, stdout, stderr)
	case "inspect":
		return runInspect(positional[1:], opts, stdout, stderr)
	case "tail":
		return runTail(positional[1:], opts, stdout, stderr)
	case "import-memory":
		return runImportMemory(positional[1:], opts, stdout, stderr)
	case "validate-memory":
		return runValidateMemory(positional[1:], opts, stdout, stderr)
	case "serve":
		return runServe(positional[1:], opts, stdout, stderr)
	case "help", "-h", "--help":
		usage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n", positional[0])
		usage(stderr)
		return 2
	}
}

func runStatus(args []string, opts options, stdout, stderr io.Writer) int {
	if len(args) != 0 {
		fmt.Fprintln(stderr, "status does not accept positional arguments")
		return 2
	}
	store, closeStore, ok := openStore(opts, stderr)
	if !ok {
		return 2
	}
	defer closeStore()
	return writeOutput(stdout, stderr, store.Status(), opts.pretty)
}

func runInspect(args []string, opts options, stdout, stderr io.Writer) int {
	if len(args) != 2 {
		fmt.Fprintln(stderr, "inspect requires a kind and id")
		return 2
	}
	store, closeStore, ok := openStore(opts, stderr)
	if !ok {
		return 2
	}
	defer closeStore()

	read, err := inspectRead(args[0], args[1], opts.includeDeleted)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	result, err := store.Read(context.Background(), read)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if !result.Found {
		fmt.Fprintf(stderr, "%s %q not found\n", args[0], args[1])
		return 1
	}
	return writeOutput(stdout, stderr, result, opts.pretty)
}

func runTail(args []string, opts options, stdout, stderr io.Writer) int {
	if len(args) != 0 {
		fmt.Fprintln(stderr, "tail does not accept positional arguments")
		return 2
	}
	store, closeStore, ok := openStore(opts, stderr)
	if !ok {
		return 2
	}
	defer closeStore()

	result, err := store.Tail(context.Background(), kilo.TailQuery{
		MinSeq: opts.minSeq,
		Limit:  opts.limit,
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return writeOutput(stdout, stderr, result, opts.pretty)
}

func runImportMemory(args []string, opts options, stdout, stderr io.Writer) int {
	if len(args) != 0 {
		fmt.Fprintln(stderr, "import-memory does not accept positional arguments")
		return 2
	}
	if opts.input == "" {
		fmt.Fprintln(stderr, "--input is required")
		return 2
	}
	store, closeStore, ok := openStoreForMutation(opts, stderr)
	if !ok {
		return 2
	}
	defer closeStore()

	vectorIndex, err := kilo.NewFileVectorIndex(kilo.FileVectorIndexOptions{
		Path:       filepath.Join(opts.path, "vectors"),
		SyncWrites: true,
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	result, err := pilotimport.ImportMemoryRecordsFromFile(context.Background(), store, opts.input, pilotimport.MemoryImportOptions{
		Namespace:   opts.namespace,
		ProjectID:   opts.projectID,
		HumanID:     opts.humanID,
		VectorIndex: vectorIndex,
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return writeOutput(stdout, stderr, result, opts.pretty)
}

func runValidateMemory(args []string, opts options, stdout, stderr io.Writer) int {
	if len(args) != 0 {
		fmt.Fprintln(stderr, "validate-memory does not accept positional arguments")
		return 2
	}
	if opts.cases == "" {
		fmt.Fprintln(stderr, "--cases is required")
		return 2
	}
	var (
		store      *kilo.Store
		closeStore func()
		ok         bool
	)
	if opts.input != "" {
		store, closeStore, ok = openStoreForMutation(opts, stderr)
	} else {
		store, closeStore, ok = openStore(opts, stderr)
	}
	if !ok {
		return 2
	}
	defer closeStore()

	vectorIndex, err := kilo.NewFileVectorIndex(kilo.FileVectorIndexOptions{
		Path:       filepath.Join(opts.path, "vectors"),
		SyncWrites: true,
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	var imported pilotimport.MemoryImportResult
	if opts.input != "" {
		imported, err = pilotimport.ImportMemoryRecordsFromFile(context.Background(), store, opts.input, pilotimport.MemoryImportOptions{
			Namespace:   opts.namespace,
			ProjectID:   opts.projectID,
			HumanID:     opts.humanID,
			VectorIndex: vectorIndex,
		})
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
	}

	casesFile, err := os.Open(opts.cases)
	if err != nil {
		fmt.Fprintf(stderr, "open validation cases: %v\n", err)
		return 1
	}
	defer casesFile.Close()
	plan, err := pilotvalidate.LoadPlan(casesFile)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if opts.limit > 0 {
		plan.Limit = opts.limit
	}
	if opts.parallelSessions > 0 {
		plan.ParallelSessions = opts.parallelSessions
	}
	if opts.repeats > 0 {
		plan.Repeats = opts.repeats
	}
	if opts.requireFresh {
		plan.RequireFresh = true
	}
	report, err := pilotvalidate.Validate(context.Background(), store, vectorIndex, plan, imported)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	code := writeOutput(stdout, stderr, report, opts.pretty)
	if code != 0 {
		return code
	}
	if report.Summary.Failed > 0 {
		return 1
	}
	return 0
}

func runServe(args []string, opts options, stdout, stderr io.Writer) int {
	if len(args) != 0 {
		fmt.Fprintln(stderr, "serve does not accept positional arguments")
		return 2
	}
	store, closeStore, ok := openStoreForServe(opts, stderr)
	if !ok {
		return 2
	}
	defer closeStore()
	vectorIndex, err := kilo.NewFileVectorIndex(kilo.FileVectorIndexOptions{
		Path:       filepath.Join(opts.path, "vectors"),
		SyncWrites: true,
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	addr := opts.addr
	if addr == "" {
		addr = "127.0.0.1:8765"
	}
	fmt.Fprintf(stdout, "serving kilo on http://%s\n", addr)
	server := &http.Server{
		Addr:              addr,
		Handler:           httpapi.NewWithOptions(store, httpapi.ServerOptions{MemoryImportVectorIndex: vectorIndex}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := server.ListenAndServe(); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func openStoreForServe(opts options, stderr io.Writer) (*kilo.Store, func(), bool) {
	if opts.initStore {
		return openStoreForMutation(opts, stderr)
	}
	return openStore(opts, stderr)
}

func openStoreForMutation(opts options, stderr io.Writer) (*kilo.Store, func(), bool) {
	if opts.path == "" {
		fmt.Fprintln(stderr, "--path is required")
		return nil, nil, false
	}
	if !opts.initStore {
		return openStore(opts, stderr)
	}
	if err := os.MkdirAll(opts.path, 0o755); err != nil {
		fmt.Fprintf(stderr, "create store path: %v\n", err)
		return nil, nil, false
	}
	store, err := kilo.Open(context.Background(), kilo.Options{
		Path:       opts.path,
		SyncWrites: true,
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return nil, nil, false
	}
	return store, func() {
		if err := store.Close(); err != nil {
			fmt.Fprintln(stderr, err)
		}
	}, true
}

func openStore(opts options, stderr io.Writer) (*kilo.Store, func(), bool) {
	if opts.path == "" {
		fmt.Fprintln(stderr, "--path is required")
		return nil, nil, false
	}
	if stat, err := os.Stat(opts.path); err != nil {
		fmt.Fprintf(stderr, "store path is not readable: %v\n", err)
		return nil, nil, false
	} else if !stat.IsDir() {
		fmt.Fprintf(stderr, "store path is not a directory: %s\n", opts.path)
		return nil, nil, false
	}
	activeSegment := filepath.Join(opts.path, "segments", "00000000000000000001.kseg")
	if _, err := os.Stat(activeSegment); err != nil {
		fmt.Fprintf(stderr, "store path is not initialized: %v\n", err)
		return nil, nil, false
	}
	store, err := kilo.Open(context.Background(), kilo.Options{
		Path:       opts.path,
		SyncWrites: opts.syncWrites,
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return nil, nil, false
	}
	return store, func() {
		if err := store.Close(); err != nil {
			fmt.Fprintln(stderr, err)
		}
	}, true
}

func inspectRead(kind, id string, includeDeleted bool) (kilo.Read, error) {
	switch strings.TrimSpace(kind) {
	case "record", "records":
		return kilo.Read{RecordID: id, IncludeDeleted: includeDeleted}, nil
	case "node", "nodes":
		return kilo.Read{NodeID: id, IncludeDeleted: includeDeleted}, nil
	case "edge", "edges":
		return kilo.Read{EdgeID: id, IncludeDeleted: includeDeleted}, nil
	case "chunk", "chunks":
		return kilo.Read{ChunkID: id}, nil
	case "chunk-content":
		return kilo.Read{ChunkContentID: id}, nil
	case "source-span", "source-spans":
		return kilo.Read{SourceSpanID: id}, nil
	case "purge-request", "purge-requests":
		return kilo.Read{PurgeRequestID: id}, nil
	default:
		return kilo.Read{}, fmt.Errorf("unsupported inspect kind %q", kind)
	}
}

func writeOutput(stdout, stderr io.Writer, value any, pretty bool) int {
	encoder := json.NewEncoder(stdout)
	if pretty {
		encoder.SetIndent("", "  ")
	}
	if err := encoder.Encode(value); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func parseArgs(args []string) ([]string, options, error) {
	opts := options{}
	positional := []string{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--path":
			i++
			if i >= len(args) {
				return nil, options{}, fmt.Errorf("--path requires a value")
			}
			opts.path = args[i]
		case strings.HasPrefix(arg, "--path="):
			opts.path = strings.TrimPrefix(arg, "--path=")
		case arg == "--addr":
			i++
			if i >= len(args) {
				return nil, options{}, fmt.Errorf("--addr requires a value")
			}
			opts.addr = args[i]
		case strings.HasPrefix(arg, "--addr="):
			opts.addr = strings.TrimPrefix(arg, "--addr=")
		case arg == "--pretty":
			opts.pretty = true
		case arg == "--include-deleted":
			opts.includeDeleted = true
		case arg == "--sync":
			opts.syncWrites = true
		case arg == "--init":
			opts.initStore = true
		case arg == "--input":
			i++
			if i >= len(args) {
				return nil, options{}, fmt.Errorf("--input requires a value")
			}
			opts.input = args[i]
		case strings.HasPrefix(arg, "--input="):
			opts.input = strings.TrimPrefix(arg, "--input=")
		case arg == "--cases":
			i++
			if i >= len(args) {
				return nil, options{}, fmt.Errorf("--cases requires a value")
			}
			opts.cases = args[i]
		case strings.HasPrefix(arg, "--cases="):
			opts.cases = strings.TrimPrefix(arg, "--cases=")
		case arg == "--require-fresh":
			opts.requireFresh = true
		case arg == "--namespace":
			i++
			if i >= len(args) {
				return nil, options{}, fmt.Errorf("--namespace requires a value")
			}
			opts.namespace = args[i]
		case strings.HasPrefix(arg, "--namespace="):
			opts.namespace = strings.TrimPrefix(arg, "--namespace=")
		case arg == "--project-id":
			i++
			if i >= len(args) {
				return nil, options{}, fmt.Errorf("--project-id requires a value")
			}
			opts.projectID = args[i]
		case strings.HasPrefix(arg, "--project-id="):
			opts.projectID = strings.TrimPrefix(arg, "--project-id=")
		case arg == "--human-id":
			i++
			if i >= len(args) {
				return nil, options{}, fmt.Errorf("--human-id requires a value")
			}
			opts.humanID = args[i]
		case strings.HasPrefix(arg, "--human-id="):
			opts.humanID = strings.TrimPrefix(arg, "--human-id=")
		case arg == "--limit":
			i++
			if i >= len(args) {
				return nil, options{}, fmt.Errorf("--limit requires a value")
			}
			limit, err := strconv.Atoi(args[i])
			if err != nil {
				return nil, options{}, fmt.Errorf("invalid --limit %q", args[i])
			}
			if limit < 0 {
				return nil, options{}, fmt.Errorf("--limit must be non-negative")
			}
			opts.limit = limit
		case strings.HasPrefix(arg, "--limit="):
			raw := strings.TrimPrefix(arg, "--limit=")
			limit, err := strconv.Atoi(raw)
			if err != nil {
				return nil, options{}, fmt.Errorf("invalid --limit %q", raw)
			}
			if limit < 0 {
				return nil, options{}, fmt.Errorf("--limit must be non-negative")
			}
			opts.limit = limit
		case arg == "--parallel-sessions":
			i++
			if i >= len(args) {
				return nil, options{}, fmt.Errorf("--parallel-sessions requires a value")
			}
			parallelSessions, err := strconv.Atoi(args[i])
			if err != nil {
				return nil, options{}, fmt.Errorf("invalid --parallel-sessions %q", args[i])
			}
			if parallelSessions < 0 {
				return nil, options{}, fmt.Errorf("--parallel-sessions must be non-negative")
			}
			opts.parallelSessions = parallelSessions
		case strings.HasPrefix(arg, "--parallel-sessions="):
			raw := strings.TrimPrefix(arg, "--parallel-sessions=")
			parallelSessions, err := strconv.Atoi(raw)
			if err != nil {
				return nil, options{}, fmt.Errorf("invalid --parallel-sessions %q", raw)
			}
			if parallelSessions < 0 {
				return nil, options{}, fmt.Errorf("--parallel-sessions must be non-negative")
			}
			opts.parallelSessions = parallelSessions
		case arg == "--repeats":
			i++
			if i >= len(args) {
				return nil, options{}, fmt.Errorf("--repeats requires a value")
			}
			repeats, err := strconv.Atoi(args[i])
			if err != nil {
				return nil, options{}, fmt.Errorf("invalid --repeats %q", args[i])
			}
			if repeats < 0 {
				return nil, options{}, fmt.Errorf("--repeats must be non-negative")
			}
			opts.repeats = repeats
		case strings.HasPrefix(arg, "--repeats="):
			raw := strings.TrimPrefix(arg, "--repeats=")
			repeats, err := strconv.Atoi(raw)
			if err != nil {
				return nil, options{}, fmt.Errorf("invalid --repeats %q", raw)
			}
			if repeats < 0 {
				return nil, options{}, fmt.Errorf("--repeats must be non-negative")
			}
			opts.repeats = repeats
		case arg == "--min-seq":
			i++
			if i >= len(args) {
				return nil, options{}, fmt.Errorf("--min-seq requires a value")
			}
			minSeq, err := strconv.ParseUint(args[i], 10, 64)
			if err != nil {
				return nil, options{}, fmt.Errorf("invalid --min-seq %q", args[i])
			}
			opts.minSeq = minSeq
		case strings.HasPrefix(arg, "--min-seq="):
			raw := strings.TrimPrefix(arg, "--min-seq=")
			minSeq, err := strconv.ParseUint(raw, 10, 64)
			if err != nil {
				return nil, options{}, fmt.Errorf("invalid --min-seq %q", raw)
			}
			opts.minSeq = minSeq
		default:
			positional = append(positional, arg)
		}
	}
	return positional, opts, nil
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage:")
	fmt.Fprintln(w, "  kilo status --path PATH [--pretty]")
	fmt.Fprintln(w, "  kilo inspect KIND ID --path PATH [--include-deleted] [--pretty]")
	fmt.Fprintln(w, "  kilo tail --path PATH [--limit N] [--min-seq N] [--pretty]")
	fmt.Fprintln(w, "  kilo import-memory --path PATH --input FILE [--init] [--namespace NAME] [--project-id ID] [--human-id ID]")
	fmt.Fprintln(w, "  kilo validate-memory --path PATH --cases FILE [--input FILE] [--init] [--parallel-sessions N] [--repeats N] [--require-fresh] [--pretty]")
	fmt.Fprintln(w, "  kilo serve --path PATH [--addr HOST:PORT] [--init]")
}
