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
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const driverName = "s3minio"

// minChunkSize defines the minimum multipart upload chunk size.
// S3 API requires multipart upload chunks to be at least 5MB.
const minChunkSize = 5 << 20

const defaultChunkSize = minChunkSize

// listMax is the largest amount of objects you can request from S3 in a list call.
const listMax = 1000

// DriverParameters is a struct that encapsulates all of the driver parameters after all values have been set.
type DriverParameters struct {
	AccessKey     string
	SecretKey     string
	Bucket        string
	Endpoint      string
	Secure        bool
	ChunkSize     int64
	RootDirectory string
}

func init() {
	factory.Register(driverName, &s3DriverFactory{})
}

func parseError(path string, err error) error {
	if e, ok := err.(minio.ErrorResponse); ok && e.Code == "NoSuchKey" {
		return storagedriver.PathNotFoundError{Path: path}
	}
	return err
}

// s3DriverFactory implements the factory.StorageDriverFactory interface
type s3DriverFactory struct{}

func (factory *s3DriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	S3            *minio.Core
	Bucket        string
	ChunkSize     int64
	RootDirectory string
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by Amazon S3
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - accesskey
// - secretkey
// - endpoint
// - bucket
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	// Providing no values for these is valid in case the user is authenticating
	// with an IAM on an ec2 instance (in which case the instance credentials will
	// be summoned when GetAuth is called)
	accessKey := parameters["accesskey"]
	if accessKey == nil {
		accessKey = ""
	}
	secretKey := parameters["secretkey"]
	if secretKey == nil {
		secretKey = ""
	}

	regionEndpoint := parameters["regionendpoint"]
	if regionEndpoint == nil {
		regionEndpoint = ""
	}

	bucket := parameters["bucket"]
	if bucket == nil || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
	}

	secureBool := true
	secure := parameters["secure"]
	switch secure := secure.(type) {
	case string:
		b, err := strconv.ParseBool(secure)
		if err != nil {
			return nil, fmt.Errorf("The secure parameter should be a boolean")
		}
		secureBool = b
	case bool:
		secureBool = secure
	case nil:
		// do nothing
	default:
		return nil, fmt.Errorf("The secure parameter should be a boolean")
	}

	chunkSize := int64(defaultChunkSize)
	chunkSizeParam := parameters["chunksize"]
	switch v := chunkSizeParam.(type) {
	case string:
		vv, err := strconv.ParseInt(v, 0, 64)
		if err != nil {
			return nil, fmt.Errorf("chunksize parameter must be an integer, %v invalid", chunkSizeParam)
		}
		chunkSize = vv
	case int64:
		chunkSize = v
	case int, uint, int32, uint32, uint64:
		chunkSize = reflect.ValueOf(v).Convert(reflect.TypeOf(chunkSize)).Int()
	case nil:
		// do nothing
	default:
		return nil, fmt.Errorf("invalid value for chunksize: %#v", chunkSizeParam)
	}

	if chunkSize < minChunkSize {
		return nil, fmt.Errorf("The chunksize %#v parameter should be a number that is larger than or equal to %d", chunkSize, minChunkSize)
	}

	rootDirectory := parameters["rootdirectory"]
	if rootDirectory == nil {
		rootDirectory = ""
	}

	params := DriverParameters{
		AccessKey:     fmt.Sprint(accessKey),
		SecretKey:     fmt.Sprint(secretKey),
		Bucket:        fmt.Sprint(bucket),
		Endpoint:      fmt.Sprint(regionEndpoint),
		Secure:        secureBool,
		ChunkSize:     chunkSize,
		RootDirectory: fmt.Sprint(rootDirectory),
	}
	return New(params)
}

// New constructs a new Driver with the given parameters.
func New(params DriverParameters) (*Driver, error) {
	s3obj, err := minio.NewCore(params.Endpoint, params.AccessKey, params.SecretKey, params.Secure)
	if err != nil {
		return nil, err
	}

	d := &driver{
		S3:            s3obj,
		Bucket:        params.Bucket,
		ChunkSize:     params.ChunkSize,
		RootDirectory: params.RootDirectory,
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	reader, err := d.Reader(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(reader)
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	_, err := d.S3.PutObject(d.Bucket, d.s3Path(path), int64(len(contents)), bytes.NewReader(contents), nil, nil, map[string][]string{
		"Content-Type": []string{d.getContentType()},
	})
	return err
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	reader, _, err := d.S3.GetObject(d.Bucket, d.s3Path(path), minio.RequestHeaders{
		Header: http.Header{
			"Range": {"bytes=" + strconv.FormatInt(offset, 10) + "-"},
		},
	})
	return reader, parseError(path, err)
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	if append {
		return resumeWriter(ctx, d, path)
	}
	return createWriter(d, path)
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	resp, err := d.S3.ListObjects(d.Bucket, d.s3Path(path), "", "", 1)
	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if len(resp.Contents) == 1 {
		if resp.Contents[0].Key != d.s3Path(path) {
			fi.IsDir = true
		} else {
			fi.IsDir = false
			fi.Size = resp.Contents[0].Size
			fi.ModTime = resp.Contents[0].LastModified
		}
	} else if len(resp.CommonPrefixes) == 1 {
		fi.IsDir = true
	} else {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, opath string) ([]string, error) {
	path := opath
	if path != "/" && path[len(path)-1] != '/' {
		path = path + "/"
	}

	// This is to cover for the cases when the rootDirectory of the driver is either "" or "/".
	// In those cases, there is no root prefix to replace and we must actually add a "/" to all
	// results in order to keep them as valid paths as recognized by storagedriver.PathRegexp
	prefix := ""
	if d.s3Path("") == "" {
		prefix = "/"
	}

	resp, err := d.S3.ListObjects(d.Bucket, d.s3Path(path), "", "/", listMax)
	if err != nil {
		return nil, err
	}

	files := []string{}
	directories := []string{}

	for {
		for _, key := range resp.Contents {
			files = append(files, strings.Replace(key.Key, d.s3Path(""), prefix, 1))
		}

		for _, commonPrefix := range resp.CommonPrefixes {
			commonPrefix := commonPrefix.Prefix
			directories = append(directories, strings.Replace(commonPrefix[0:len(commonPrefix)-1], d.s3Path(""), prefix, 1))
		}

		if resp.IsTruncated {
			resp, err = d.S3.ListObjects(d.Bucket, d.s3Path(path), resp.NextMarker, "/", listMax)
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}

	if opath != "/" {
		if len(files) == 0 && len(directories) == 0 {
			// Treat empty response as missing directory, since we don't actually
			// have directories in s3.
			return nil, storagedriver.PathNotFoundError{Path: opath}
		}
	}

	return append(files, directories...), nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	/* This is terrible, but aws doesn't have an actual move. */
	dst, err := minio.NewDestinationInfo(d.Bucket, d.s3Path(destPath), nil, nil)
	if err != nil {
		return err
	}

	src := minio.NewSourceInfo(d.Bucket, d.s3Path(sourcePath), nil)

	err = d.S3.CopyObject(dst, src)
	if err != nil {
		return err
	}

	return d.Delete(ctx, sourcePath)
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	objectsCh := make(chan string)
	errorCh := d.S3.RemoveObjects(d.Bucket, objectsCh)

	var removeErr error
	done := make(chan struct{})
	go func() {
		removeObjectError, ok := <-errorCh
		if ok {
			removeErr = removeObjectError.Err
		}
		close(done)
		for range errorCh {
		}
	}()

Loop:
	for {
		resp, err := d.S3.ListObjects(d.Bucket, d.s3Path(path), "", "", 0)
		if err != nil {
			close(objectsCh)
			return err
		}
		if len(resp.Contents) == 0 {
			break
		}
		for _, key := range resp.Contents {
			select {
			case <-ctx.Done():
				break Loop
			case <-done:
				break Loop
			case objectsCh <- key.Key:
			}
		}
	}
	close(objectsCh)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	return removeErr
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	method, ok := options["method"]
	if ok {
		methodString, ok := method.(string)
		if !ok || methodString != "GET" {
			return "", storagedriver.ErrUnsupportedMethod{}
		}
	}

	expiresIn := 20 * time.Minute
	expires, ok := options["expiry"]
	if ok {
		et, ok := expires.(time.Time)
		if ok {
			expiresIn = et.Sub(time.Now())
		}
	}

	u, err := d.S3.PresignedGetObject(d.Bucket, d.s3Path(path), expiresIn, nil)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func (d *driver) s3Path(path string) string {
	return strings.TrimLeft(strings.TrimRight(d.RootDirectory, "/")+path, "/")
}

func (d *driver) getContentType() string {
	return "application/octet-stream"
}

func (d *driver) getACL() string {
	return "private"
}
