// Package diskcache provides a disk-backed caching wrapper for objstore.BucketReader.
// Objects fetched via Get or GetRange are stored as local files so subsequent
// reads can be served without network I/O.
package diskcache

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/thanos-io/objstore"
	"golang.org/x/sync/singleflight"
)

// BucketReader wraps an objstore.BucketReader and caches object data on the
// local filesystem.
//
// Get stores the complete object at <cacheDir>/<name>.
//
// GetRange fetches only the requested byte range from the underlying bucket
// and stores it at <cacheDir>/<name>.ranges/<off>-<length>.  Each distinct
// (off, length) pair is its own file; a cache hit requires an exact match.
//
// All other BucketReader methods (Iter, Exists, Attributes, …) are forwarded
// to the underlying reader without caching.
type BucketReader struct {
	wrapped  objstore.BucketReader
	cacheDir string
	group    singleflight.Group
}

// New returns a BucketReader that caches fetched objects under cacheDir.
// cacheDir is created lazily the first time an object is written.
func New(wrapped objstore.BucketReader, cacheDir string) *BucketReader {
	return &BucketReader{
		wrapped:  wrapped,
		cacheDir: cacheDir,
	}
}

// Ensure BucketReader satisfies the interface at compile time.
var _ objstore.BucketReader = (*BucketReader)(nil)

// cachePath maps an object name to the local path used by Get.
func (c *BucketReader) cachePath(name string) string {
	return filepath.Join(c.cacheDir, filepath.FromSlash(name))
}

// rangePath maps an object name and byte range to a local file path used by
// GetRange.  The files live in a "<name>.ranges/" directory so they cannot
// collide with the full-object file written by Get.
func (c *BucketReader) rangePath(name string, off, length int64) string {
	dir := filepath.Join(c.cacheDir, filepath.FromSlash(name)+".ranges")
	return filepath.Join(dir, fmt.Sprintf("%d-%d", off, length))
}

// Get returns a reader for the named object.
// If the object is already cached on disk the local file is opened and
// returned immediately.  Otherwise the object is downloaded from the
// underlying bucket, written to disk, and then the local file is returned.
func (c *BucketReader) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	path := c.cachePath(name)
	if f, err := os.Open(path); err == nil {
		return f, nil
	}
	_, err, _ := c.group.Do(name, func() (any, error) {
		rc, err := c.wrapped.Get(ctx, name)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return nil, writeToCache(path, rc)
	})
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

// GetRange returns a reader for [off, off+length) of the named object.
// If that exact range is already cached on disk it is returned directly.
// Otherwise only the requested range is fetched from the underlying bucket,
// stored as its own cache file, and then returned from the local copy.
func (c *BucketReader) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	path := c.rangePath(name, off, length)
	if f, err := os.Open(path); err == nil {
		return f, nil
	}
	key := fmt.Sprintf("%s@%d-%d", name, off, length)
	_, err, _ := c.group.Do(key, func() (any, error) {
		rc, err := c.wrapped.GetRange(ctx, name, off, length)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return nil, writeToCache(path, rc)
	})
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

// writeToCache writes all data from r into the file at path, creating any
// intermediate directories as needed.  A temporary file is used so that a
// partial write is never visible as a completed cache entry.
func writeToCache(path string, r io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// Pass-through methods – these are forwarded directly to the underlying reader.

func (c *BucketReader) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	return c.wrapped.Iter(ctx, dir, f, options...)
}

func (c *BucketReader) IterWithAttributes(ctx context.Context, dir string, f func(objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	return c.wrapped.IterWithAttributes(ctx, dir, f, options...)
}

func (c *BucketReader) SupportedIterOptions() []objstore.IterOptionType {
	return c.wrapped.SupportedIterOptions()
}

func (c *BucketReader) Exists(ctx context.Context, name string) (bool, error) {
	return c.wrapped.Exists(ctx, name)
}

func (c *BucketReader) IsObjNotFoundErr(err error) bool {
	return c.wrapped.IsObjNotFoundErr(err)
}

func (c *BucketReader) IsAccessDeniedErr(err error) bool {
	return c.wrapped.IsAccessDeniedErr(err)
}

func (c *BucketReader) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	return c.wrapped.Attributes(ctx, name)
}
