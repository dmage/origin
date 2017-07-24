// Package s3 provides a storagedriver.StorageDriver implementation to
// store blobs in Amazon S3 cloud storage.
//
// This package leverages the official aws client library for interfacing with
// S3.
//
// Because S3 is a key, value store the Stat call does not support last modification
// time for directories (directories are an abstraction for key, value stores)
//
// Keep in mind that S3 guarantees only read-after-write consistency for new
// objects, but no read-after-update or list-after-write consistency.
package s3

import (
	"fmt"
	"io"
	"sort"

	log "github.com/Sirupsen/logrus"
	"github.com/minio/minio-go"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
)

const chunkSize = 5 << 20

type completedParts []minio.CompletePart

func (a completedParts) Len() int           { return len(a) }
func (a completedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a completedParts) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

type zeroReader struct {
}

func (r zeroReader) Read(buf []byte) (int, error) {
	for i := range buf {
		buf[i] = 0
	}
	return len(buf), nil
}

type zeroPaddedWriter struct {
	Size int
	W    io.WriteCloser
	n    int
}

func (w *zeroPaddedWriter) Write(buf []byte) (int, error) {
	n, err := w.W.Write(buf)
	w.n += n
	return n, err
}

func (w *zeroPaddedWriter) Close() error {
	if w.n < w.Size {
		log.Println("PADDING", int64(w.Size-w.n))
		n, err := io.Copy(w.W, io.LimitReader(zeroReader{}, int64(w.Size-w.n)))
		w.n += int(n)
		if err != nil {
			return err
		}
	}
	return w.W.Close()
}

type objectPartWriter struct {
	io.WriteCloser
	done   chan struct{}
	putErr error
}

func (w *objectPartWriter) Close() error {
	err := w.WriteCloser.Close()
	if err != nil {
		return err
	}
	<-w.done
	return w.putErr
}

// writer attempts to upload parts to S3 in a buffered fashion where the last
// part is at least as large as the chunksize, so the multipart upload could be
// cleanly resumed in the future. This is violated if Close is called after less
// than a full chunk is written.
type writer struct {
	driver    *driver
	chunker   Chunker
	key       string
	uploadID  string
	parts     []minio.ObjectPart
	size      int64
	closed    bool
	committed bool
	cancelled bool
}

func newWriter(d *driver, key, uploadID string, parts []minio.ObjectPart, size int64) storagedriver.FileWriter {
	w := &writer{
		driver:   d,
		key:      key,
		uploadID: uploadID,
		parts:    parts,
		size:     size,
	}
	w.chunker = Chunker{
		Size: chunkSize,
		New: func() io.WriteCloser {
			partWriter := &objectPartWriter{
				done: make(chan struct{}),
			}
			pipeReader, pipeWriter := io.Pipe()
			go func() {
				var part minio.ObjectPart
				part, partWriter.putErr = d.S3.PutObjectPart(d.Bucket, key, uploadID, len(w.parts)+1, chunkSize, pipeReader, nil, nil)
				w.parts = append(w.parts, part)
				close(partWriter.done)
			}()
			partWriter.WriteCloser = &zeroPaddedWriter{
				W:    pipeWriter,
				Size: int(chunkSize),
			}
			return partWriter
		},
	}
	return w
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("already closed")
	} else if w.committed {
		return 0, fmt.Errorf("already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	n, err := w.chunker.Write(p)
	w.size += int64(n)
	return n, err
}

func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}
	w.closed = true

	// TODO
	w.driver.PutContent(context.Background(), w.key+".size", []byte(fmt.Sprintf("%d", w.size)))

	return w.chunker.Close()
}

func (w *writer) Cancel() error {
	/*
		if w.closed {
			return fmt.Errorf("already closed")
		} else if w.committed {
			return fmt.Errorf("already committed")
		}
		w.cancelled = true
		_, err := w.driver.S3.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   aws.String(w.driver.Bucket),
			Key:      aws.String(w.key),
			UploadId: aws.String(w.uploadID),
		})
		return err
	*/
	return nil
}

func (w *writer) Commit() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}
	w.committed = true

	var completedUploadedParts completedParts
	for _, part := range w.parts {
		completedUploadedParts = append(completedUploadedParts, minio.CompletePart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		})
	}

	sort.Sort(completedUploadedParts)

	err := w.driver.S3.CompleteMultipartUpload(w.driver.Bucket, w.key, w.uploadID, completedUploadedParts)
	if err != nil {
		/*
			w.driver.S3.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
				Bucket:   aws.String(w.driver.Bucket),
				Key:      aws.String(w.key),
				UploadId: aws.String(w.uploadID),
			})
		*/
		return err
	}

	dst, err := minio.NewDestinationInfo(w.driver.Bucket, w.key, nil, nil)
	if err != nil {
		return err
	}
	src := minio.NewSourceInfo(w.driver.Bucket, w.key, nil)
	src.SetRange(0, w.size-1)
	err = w.driver.S3.CopyObject(dst, src)
	if err != nil {
		return err
	}

	return nil
}
