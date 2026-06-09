package diskcache

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

// fakeBucket is a minimal in-memory BucketReader used in tests.
type fakeBucket struct {
	mu      sync.Mutex
	objects map[string][]byte
	gets    int // number of times Get/GetRange was called on the remote

	// If non-nil, Get/GetRange blocks until this channel is closed.
	block chan struct{}
}

func newFakeBucket(objects map[string][]byte) *fakeBucket {
	return &fakeBucket{objects: objects}
}

func (f *fakeBucket) Get(_ context.Context, name string) (io.ReadCloser, error) {
	data, ok := f.objects[name]
	if !ok {
		return nil, errNotFound(name)
	}
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	f.gets++
	f.mu.Unlock()
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *fakeBucket) GetRange(_ context.Context, name string, off, length int64) (io.ReadCloser, error) {
	data, ok := f.objects[name]
	if !ok {
		return nil, errNotFound(name)
	}
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	f.gets++
	f.mu.Unlock()
	end := off + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return io.NopCloser(bytes.NewReader(data[off:end])), nil
}

func (f *fakeBucket) Iter(_ context.Context, _ string, fn func(string) error, _ ...objstore.IterOption) error {
	for name := range f.objects {
		if err := fn(name); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeBucket) IterWithAttributes(_ context.Context, _ string, fn func(objstore.IterObjectAttributes) error, _ ...objstore.IterOption) error {
	for name := range f.objects {
		if err := fn(objstore.IterObjectAttributes{Name: name}); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeBucket) SupportedIterOptions() []objstore.IterOptionType { return nil }
func (f *fakeBucket) Exists(_ context.Context, name string) (bool, error) {
	_, ok := f.objects[name]
	return ok, nil
}
func (f *fakeBucket) IsObjNotFoundErr(err error) bool  { return err != nil && err.Error() == "not found" }
func (f *fakeBucket) IsAccessDeniedErr(_ error) bool   { return false }
func (f *fakeBucket) Attributes(_ context.Context, name string) (objstore.ObjectAttributes, error) {
	data, ok := f.objects[name]
	if !ok {
		return objstore.ObjectAttributes{}, errNotFound(name)
	}
	return objstore.ObjectAttributes{Size: int64(len(data))}, nil
}

type notFoundErr string

func (e notFoundErr) Error() string { return "not found" }
func errNotFound(name string) error { return notFoundErr(name) }

func TestGet_CacheMiss(t *testing.T) {
	ctx := context.Background()
	data := []byte("hello, world")
	remote := newFakeBucket(map[string][]byte{"obj/foo": data})

	cache := New(remote, t.TempDir())

	rc, err := cache.Get(ctx, "obj/foo")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	require.Equal(t, data, got)
	require.Equal(t, 1, remote.gets, "expected exactly one remote fetch")
}

func TestGet_CacheHit(t *testing.T) {
	ctx := context.Background()
	data := []byte("hello, world")
	remote := newFakeBucket(map[string][]byte{"obj/foo": data})
	dir := t.TempDir()
	cache := New(remote, dir)

	// First call populates cache.
	rc, err := cache.Get(ctx, "obj/foo")
	require.NoError(t, err)
	_, _ = io.ReadAll(rc)
	_ = rc.Close()

	// Second call should be served from disk.
	rc2, err := cache.Get(ctx, "obj/foo")
	require.NoError(t, err)
	got, err := io.ReadAll(rc2)
	require.NoError(t, err)
	require.NoError(t, rc2.Close())

	require.Equal(t, data, got)
	require.Equal(t, 1, remote.gets, "second Get should not hit remote")
}

func TestGet_FileCreated(t *testing.T) {
	ctx := context.Background()
	data := []byte("cached content")
	remote := newFakeBucket(map[string][]byte{"a/b/c": data})
	dir := t.TempDir()
	cache := New(remote, dir)

	rc, err := cache.Get(ctx, "a/b/c")
	require.NoError(t, err)
	_, _ = io.ReadAll(rc)
	_ = rc.Close()

	expectedPath := filepath.Join(dir, "a", "b", "c")
	content, err := os.ReadFile(expectedPath)
	require.NoError(t, err)
	require.Equal(t, data, content)
}

func TestGetRange_CacheMiss(t *testing.T) {
	ctx := context.Background()
	data := []byte("0123456789")
	remote := newFakeBucket(map[string][]byte{"obj": data})
	cache := New(remote, t.TempDir())

	rc, err := cache.GetRange(ctx, "obj", 2, 4)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	require.Equal(t, []byte("2345"), got)
	require.Equal(t, 1, remote.gets)
}

func TestGetRange_CacheHit(t *testing.T) {
	ctx := context.Background()
	data := []byte("0123456789")
	remote := newFakeBucket(map[string][]byte{"obj": data})
	cache := New(remote, t.TempDir())

	// First call fetches from remote.
	rc, err := cache.GetRange(ctx, "obj", 2, 4)
	require.NoError(t, err)
	_, _ = io.ReadAll(rc)
	_ = rc.Close()

	// Same range again: served from disk.
	rc2, err := cache.GetRange(ctx, "obj", 2, 4)
	require.NoError(t, err)
	got, err := io.ReadAll(rc2)
	require.NoError(t, err)
	require.NoError(t, rc2.Close())

	require.Equal(t, []byte("2345"), got)
	require.Equal(t, 1, remote.gets, "second call for same range should not hit remote")
}

func TestGetRange_DifferentRangesFetchedSeparately(t *testing.T) {
	ctx := context.Background()
	data := []byte("0123456789")
	remote := newFakeBucket(map[string][]byte{"obj": data})
	cache := New(remote, t.TempDir())

	rc, err := cache.GetRange(ctx, "obj", 0, 3)
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	require.Equal(t, []byte("012"), got)

	rc2, err := cache.GetRange(ctx, "obj", 7, 3)
	require.NoError(t, err)
	got2, _ := io.ReadAll(rc2)
	_ = rc2.Close()
	require.Equal(t, []byte("789"), got2)

	// Both ranges must have been fetched from remote independently.
	require.Equal(t, 2, remote.gets)
}

func TestGetRange_FileCreated(t *testing.T) {
	ctx := context.Background()
	data := []byte("0123456789")
	remote := newFakeBucket(map[string][]byte{"a/b": data})
	dir := t.TempDir()
	cache := New(remote, dir)

	rc, err := cache.GetRange(ctx, "a/b", 3, 4)
	require.NoError(t, err)
	_, _ = io.ReadAll(rc)
	_ = rc.Close()

	expectedPath := filepath.Join(dir, "a", "b.ranges", "3-4")
	content, err := os.ReadFile(expectedPath)
	require.NoError(t, err)
	require.Equal(t, []byte("3456"), content)
}

func TestGetRange_IndependentOfGet(t *testing.T) {
	ctx := context.Background()
	data := []byte("abcdefghij")
	remote := newFakeBucket(map[string][]byte{"obj": data})
	cache := New(remote, t.TempDir())

	// Prime cache via Get.
	rc, err := cache.Get(ctx, "obj")
	require.NoError(t, err)
	_, _ = io.ReadAll(rc)
	_ = rc.Close()
	require.Equal(t, 1, remote.gets)

	// GetRange has its own cache; it does not reuse the Get cache file.
	rc2, err := cache.GetRange(ctx, "obj", 3, 4)
	require.NoError(t, err)
	got, err := io.ReadAll(rc2)
	require.NoError(t, err)
	require.NoError(t, rc2.Close())
	require.Equal(t, []byte("defg"), got)
	require.Equal(t, 2, remote.gets, "GetRange fetches from remote independently of Get")
}

func TestGet_NotFound(t *testing.T) {
	ctx := context.Background()
	remote := newFakeBucket(map[string][]byte{})
	cache := New(remote, t.TempDir())

	_, err := cache.Get(ctx, "missing")
	require.Error(t, err)
	require.True(t, cache.IsObjNotFoundErr(err))
}

func TestPassThrough_Iter(t *testing.T) {
	ctx := context.Background()
	remote := newFakeBucket(map[string][]byte{"a": nil, "b": nil})
	cache := New(remote, t.TempDir())

	var names []string
	err := cache.Iter(ctx, "", func(name string) error {
		names = append(names, name)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, names, 2)
}

func TestPassThrough_Exists(t *testing.T) {
	ctx := context.Background()
	remote := newFakeBucket(map[string][]byte{"present": []byte("x")})
	cache := New(remote, t.TempDir())

	ok, err := cache.Exists(ctx, "present")
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = cache.Exists(ctx, "absent")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestGet_ConcurrentDeduplicated(t *testing.T) {
	ctx := context.Background()
	data := []byte("hello, world")
	remote := newFakeBucket(map[string][]byte{"obj": data})
	remote.block = make(chan struct{})
	cache := New(remote, t.TempDir())

	const goroutines = 10
	var ready, done sync.WaitGroup
	ready.Add(goroutines)
	done.Add(goroutines)

	// Count how many goroutines got the right data back.
	var successes atomic.Int32

	for range goroutines {
		go func() {
			ready.Done() // signal that this goroutine is about to call Get
			rc, err := cache.Get(ctx, "obj")
			defer done.Done()
			if err != nil {
				return
			}
			got, _ := io.ReadAll(rc)
			_ = rc.Close()
			if bytes.Equal(got, data) {
				successes.Add(1)
			}
		}()
	}

	// Wait until all goroutines have started, then unblock the remote.
	ready.Wait()
	close(remote.block)
	done.Wait()

	require.Equal(t, int32(goroutines), successes.Load(), "all goroutines should receive correct data")
	require.Equal(t, 1, remote.gets, "only one fetch should reach the remote")
}

func TestGetRange_ConcurrentDeduplicated(t *testing.T) {
	ctx := context.Background()
	data := []byte("0123456789")
	remote := newFakeBucket(map[string][]byte{"obj": data})
	remote.block = make(chan struct{})
	cache := New(remote, t.TempDir())

	const goroutines = 10
	var ready, done sync.WaitGroup
	ready.Add(goroutines)
	done.Add(goroutines)

	var successes atomic.Int32

	for range goroutines {
		go func() {
			ready.Done()
			rc, err := cache.GetRange(ctx, "obj", 3, 4)
			defer done.Done()
			if err != nil {
				return
			}
			got, _ := io.ReadAll(rc)
			_ = rc.Close()
			if bytes.Equal(got, []byte("3456")) {
				successes.Add(1)
			}
		}()
	}

	ready.Wait()
	close(remote.block)
	done.Wait()

	require.Equal(t, int32(goroutines), successes.Load(), "all goroutines should receive correct data")
	require.Equal(t, 1, remote.gets, "only one fetch should reach the remote")
}
