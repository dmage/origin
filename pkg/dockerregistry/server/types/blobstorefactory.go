package types

import "github.com/docker/distribution"

type BlobStoreFactory interface {
	WrapBlobStore(bs distribution.BlobStore) distribution.BlobStore
}
