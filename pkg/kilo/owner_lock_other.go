//go:build !unix

package kilo

import (
	"fmt"
	"os"
)

func lockStoreOwnerFile(file *os.File) error {
	return fmt.Errorf("%w: store owner file locks are not implemented on this platform", ErrStoreLocked)
}

func unlockStoreOwnerFile(file *os.File) error {
	return nil
}
