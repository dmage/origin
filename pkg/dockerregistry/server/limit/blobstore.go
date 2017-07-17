package limit

import (
	"runtime"
	"sync/atomic"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"

	"github.com/openshift/origin/pkg/dockerregistry/server/types"
)

type blobWriter struct {
	distribution.BlobWriter
	release func()
}

var _ distribution.BlobWriter = blobWriter{}

func (bw blobWriter) Close() error {
	bw.release()
	return bw.BlobWriter.Close()
}

type blobStore struct {
	distribution.BlobStore
	sem chan struct{}
}

var _ distribution.BlobStore = blobStore{}

func (bs blobStore) acquire() func() {
	// LOCK_BEGIN++
	sem := bs.sem
	// LOCK_WAIT_BEGIN++
	sem <- struct{}{}
	// LOCK_WAIT_END++
	done := int32(0)
	return func() {
		if atomic.SwapInt32(&done, 1) == 0 {
			<-sem
			sem = nil
			// LOCK_END++
		}
	}
}

func (bs blobStore) Create(ctx context.Context, options ...distribution.BlobCreateOption) (distribution.BlobWriter, error) {
	release := bs.acquire()

	bw, err := bs.BlobStore.Create(ctx, options...)
	if err != nil {
		release()
		return bw, err
	}

	blobwriter := blobWriter{
		BlobWriter: bw,
		release:    release,
	}
	runtime.SetFinalizer(blobwriter, func(*blobWriter) { release() })
	return blobwriter, nil
}

func (bs blobStore) Resume(ctx context.Context, id string) (distribution.BlobWriter, error) {
	release := bs.acquire()

	bw, err := bs.BlobStore.Resume(ctx, id)
	if err != nil {
		release()
		return bw, err
	}

	blobwriter := &blobWriter{
		BlobWriter: bw,
		release:    release,
	}
	runtime.SetFinalizer(blobwriter, func(*blobWriter) { release() })
	return blobwriter, nil
}

type blobStoreFactory struct {
	sem chan struct{}
}

var _ types.BlobStoreFactory = blobStoreFactory{}

func NewBlobStoreFactory(limit int) types.BlobStoreFactory {
	return blobStoreFactory{
		sem: make(chan struct{}, limit),
	}
}

func (f blobStoreFactory) WrapBlobStore(bs distribution.BlobStore) distribution.BlobStore {
	return blobStore{
		BlobStore: bs,
		sem:       f.sem,
	}
}
