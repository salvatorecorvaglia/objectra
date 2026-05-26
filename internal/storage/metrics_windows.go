//go:build windows

package storage

import (
	"syscall"
)

// GetDiskSpace retrieves total and free space of the filesystem containing path on Windows.
func GetDiskSpace(path string) (total uint64, free uint64, err error) {
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, err
	}
	var freeBytes, totalBytes, totalFreeBytes uint64
	err = syscall.GetDiskFreeSpaceEx(pathPtr, &freeBytes, &totalBytes, &totalFreeBytes)
	if err != nil {
		return 0, 0, err
	}
	return totalBytes, freeBytes, nil
}
