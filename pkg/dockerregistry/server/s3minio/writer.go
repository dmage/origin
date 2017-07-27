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
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/minio/minio-go"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
)

const chunkSize = 5 << 20

type completedParts []minio.CompletePart

func (a completedParts) Len() int           { return len(a) }
func (a completedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a completedParts) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

type writerState int

const (
	writerOpen writerState = iota
	writerClosed
	writerCommitted
	writerCancelled
)

func (s writerState) Err() error {
	switch s {
	case writerOpen:
		return nil
	case writerClosed:
		return fmt.Errorf("writer is closed")
	case writerCommitted:
		return fmt.Errorf("writer is committed")
	case writerCancelled:
		return fmt.Errorf("writer is cancelled")
	}
	return fmt.Errorf("writer is in an unknown state")
}

type writerMeta struct {
	Generation int
	Size       int64
}

func writerMetaObject(path string) string {
	return path + ".meta"
}

func loadWriterMeta(ctx context.Context, d *driver, path string) (writerMeta, error) {
	var meta writerMeta

	data, err := d.GetContent(ctx, writerMetaObject(path))
	if err != nil {
		return meta, err
	}

	err = json.Unmarshal(data, &meta)
	return meta, err
}

func (m writerMeta) store(ctx context.Context, d *driver, path string) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}

	return d.PutContent(ctx, writerMetaObject(path), data)
}

// writer attempts to upload parts to S3 in a buffered fashion where the last
// part is at least as large as the chunksize, so the multipart upload could be
// cleanly resumed in the future. This is violated if Close is called after less
// than a full chunk is written.
type writer struct {
	driver   *driver
	chunker  Chunker
	path     string
	meta     writerMeta
	state    writerState
	uploadID string
	parts    []minio.ObjectPart
}

// objectPartWriter uploads a part to S3. On successful upload ObjectPart is
// appended to objectPartWriter.w.parts.
type objectPartWriter struct {
	w    *writer
	done chan struct{}
	part minio.ObjectPart
	err  error
	io.WriteCloser
}

// newObjectPartWriter returns a writer that uploads a part to S3. If less than
// chunkSize bytes are written, the part will be padded with zero bytes.
func newObjectPartWriter(w *writer, chunkSize int64) *objectPartWriter {
	opw := &objectPartWriter{
		w:    w,
		done: make(chan struct{}),
	}
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		opw.part, opw.err = w.driver.S3.PutObjectPart(w.driver.Bucket, w.driver.s3Path(w.path), w.uploadID, len(w.parts)+1, chunkSize, pipeReader, nil, nil)
		close(opw.done)
	}()
	opw.WriteCloser = &zeroPaddedWriter{
		W:    pipeWriter,
		Size: chunkSize,
	}
	return opw
}

func (opw *objectPartWriter) Close() error {
	err := opw.WriteCloser.Close()
	if err != nil {
		return err
	}
	<-opw.done
	if opw.err != nil {
		opw.w.parts = append(opw.w.parts, opw.part)
	}
	return opw.err
}

func newWriter(d *driver, path string, meta writerMeta) storagedriver.FileWriter {
	w := &writer{
		driver: d,
		path:   path,
		meta:   meta,
	}
	w.chunker = Chunker{
		Size: chunkSize,
		New: func() io.WriteCloser {
			return newObjectPartWriter(w, chunkSize)
		},
	}
	return w
}

func (w *writer) Size() int64 {
	return w.meta.Size
}

func (w *writer) Write(p []byte) (int, error) {
	if err := w.ensureUploadID(); err != nil {
		return 0, err
	}

	n, err := w.chunker.Write(p)
	w.meta.Size += int64(n)
	return n, err
}

func (w *writer) generationPath() string {
	return fmt.Sprintf("%s.gen-%d", w.path, w.meta.Generation)
}

func (w *writer) ensureUploadID() error {
	if w.uploadID != "" {
		return nil
	}

	w.meta.Generation++

	uploadID, err := w.driver.S3.NewMultipartUpload(w.driver.Bucket, w.driver.s3Path(w.generationPath()), nil)
	if err != nil {
		w.meta.Generation--
		return err
	}

	w.uploadID = uploadID

	if w.meta.Generation > 1 {
		return fmt.Errorf("TODO: copy data from the previous generation")
	}

	return nil
}

func (w *writer) abort() error {
	/*
		w.driver.S3.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   aws.String(w.driver.Bucket),
			Key:      aws.String(w.key),
			UploadId: aws.String(w.uploadID),
		})
	*/
	return nil
}

func (w *writer) complete(ctx context.Context) error {
	var completedUploadedParts completedParts
	for _, part := range w.parts {
		completedUploadedParts = append(completedUploadedParts, minio.CompletePart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		})
	}

	sort.Sort(completedUploadedParts)

	err := w.driver.S3.CompleteMultipartUpload(w.driver.Bucket, w.driver.s3Path(w.path), w.uploadID, completedUploadedParts)
	if err != nil {
		// best effort cleanup
		_ = w.abort()
		return err
	}

	w.uploadID = ""

	return w.meta.store(ctx, w.driver, w.path)

	// TODO: remove previous .gen object
}

func (w *writer) Close() error {
	if err := w.state.Err(); err != nil {
		return err
	}

	if err := w.complete(context.Background()); err != nil {
		return err
	}

	w.state = writerClosed

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
	if err := w.state.Err(); err != nil {
		return err
	}

	if err := w.complete(context.Background()); err != nil {
		return err
	}

	dst, err := minio.NewDestinationInfo(w.driver.Bucket, w.driver.s3Path(w.path), nil, nil)
	if err != nil {
		return err
	}

	src := minio.NewSourceInfo(w.driver.Bucket, w.driver.s3Path(w.path), nil)
	src.SetRange(0, w.meta.Size-1)

	if err := w.driver.S3.CopyObject(dst, src); err != nil {
		return err
	}

	w.state = writerCommitted

	// TODO: remove .gen and .meta objects

	return nil
}

func createWriter(d *driver, path string) (storagedriver.FileWriter, error) {
	return newWriter(d, path, writerMeta{}), nil
}

func resumeWriter(ctx context.Context, d *driver, path string) (storagedriver.FileWriter, error) {
	meta, err := loadWriterMeta(ctx, d, path)
	if err != nil {
		return nil, err
	}

	return newWriter(d, path, meta), nil
}
