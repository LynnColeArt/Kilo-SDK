package kilo

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const ownerLockName = "owner.lock"

var (
	openOwnerLocksMu sync.Mutex
	openOwnerLocks   = map[string]struct{}{}
)

type storeOwnerLock struct {
	path     string
	lockPath string
	file     *os.File
	owner    StoreOwner
	closed   bool
}

func acquireStoreOwnerLock(path string, openedAt time.Time, syncWrites bool) (*storeOwnerLock, error) {
	canonicalPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve store path: %w", err)
	}
	if evaluated, err := filepath.EvalSymlinks(canonicalPath); err == nil {
		canonicalPath = evaluated
	}
	lockPath := filepath.Join(canonicalPath, ownerLockName)

	openOwnerLocksMu.Lock()
	if _, ok := openOwnerLocks[canonicalPath]; ok {
		openOwnerLocksMu.Unlock()
		return nil, fmt.Errorf("%w: store %q is already open in this process", ErrStoreLocked, canonicalPath)
	}
	openOwnerLocks[canonicalPath] = struct{}{}
	openOwnerLocksMu.Unlock()

	releaseRegistry := true
	defer func() {
		if releaseRegistry {
			releaseStoreOwnerRegistry(canonicalPath)
		}
	}()

	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open store owner lock: %w", err)
	}
	closeFile := true
	defer func() {
		if closeFile {
			_ = file.Close()
		}
	}()

	if err := lockStoreOwnerFile(file); err != nil {
		existing := readExistingStoreOwner(lockPath)
		if existing.PID != 0 {
			return nil, fmt.Errorf("%w: store %q is already owned by pid %d on %s", ErrStoreLocked, canonicalPath, existing.PID, existing.Hostname)
		}
		return nil, fmt.Errorf("%w: store %q is already owned", ErrStoreLocked, canonicalPath)
	}
	unlockFile := true
	defer func() {
		if unlockFile {
			_ = unlockStoreOwnerFile(file)
		}
	}()

	hostname, _ := os.Hostname()
	owner := StoreOwner{
		PID:      os.Getpid(),
		Hostname: hostname,
		Path:     canonicalPath,
		LockPath: lockPath,
		OpenedAt: openedAt.UTC(),
	}
	if err := writeStoreOwner(file, owner, syncWrites); err != nil {
		return nil, err
	}

	releaseRegistry = false
	closeFile = false
	unlockFile = false
	return &storeOwnerLock{
		path:     canonicalPath,
		lockPath: lockPath,
		file:     file,
		owner:    owner,
	}, nil
}

func (l *storeOwnerLock) Close() error {
	if l == nil || l.closed {
		return nil
	}
	l.closed = true
	var err error
	if l.file != nil {
		if truncateErr := l.file.Truncate(0); truncateErr != nil && err == nil {
			err = fmt.Errorf("clear store owner lock: %w", truncateErr)
		}
		if unlockErr := unlockStoreOwnerFile(l.file); unlockErr != nil && err == nil {
			err = unlockErr
		}
		if closeErr := l.file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		l.file = nil
	}
	releaseStoreOwnerRegistry(l.path)
	return err
}

func (l *storeOwnerLock) Owner() StoreOwner {
	if l == nil {
		return StoreOwner{}
	}
	return l.owner
}

func writeStoreOwner(file *os.File, owner StoreOwner, syncWrites bool) error {
	encoded, err := json.MarshalIndent(owner, "", "  ")
	if err != nil {
		return fmt.Errorf("encode store owner lock: %w", err)
	}
	if err := file.Truncate(0); err != nil {
		return fmt.Errorf("truncate store owner lock: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek store owner lock: %w", err)
	}
	if err := writeAll(file, append(encoded, '\n')); err != nil {
		return fmt.Errorf("write store owner lock: %w", err)
	}
	if syncWrites {
		if err := file.Sync(); err != nil {
			return fmt.Errorf("sync store owner lock: %w", err)
		}
	}
	return nil
}

func readExistingStoreOwner(path string) StoreOwner {
	file, err := os.Open(path)
	if err != nil {
		return StoreOwner{}
	}
	defer file.Close()
	var owner StoreOwner
	if err := json.NewDecoder(io.LimitReader(file, 64*1024)).Decode(&owner); err != nil {
		return StoreOwner{}
	}
	return owner
}

func releaseStoreOwnerRegistry(path string) {
	openOwnerLocksMu.Lock()
	delete(openOwnerLocks, path)
	openOwnerLocksMu.Unlock()
}
