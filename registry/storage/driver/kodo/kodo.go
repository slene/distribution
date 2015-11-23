// Package kodo provides a storagedriver.StorageDriver implementation to
// store blobs in Qiniu KODO cloud storage.
//
// Because KODO is a key, value store the Stat call does not support last modification
// time for directories (directories are an abstraction for key, value stores)
//
// +build include_kodo

package kodo

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"qiniupkg.com/api.v7/kodo"
	"qiniupkg.com/x/rpc.v7"

	qiniurs "qbox.us/api/rs.v3"
	qiniuup "qbox.us/api/up"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const driverName = "kodo"
const listMax = 1000
const defaultExpiry = 3600

// DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	Zone          int
	Bucket        string
	BaseURL       string
	RootDirectory string
	kodo.Config
}

func init() {
	factory.Register(driverName, &kodoDriverFactory{})
}

type kodoDriverFactory struct {
}

func (factory *kodoDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

func FromParameters(parameters map[string]interface{}) (*Driver, error) {

	var ok bool

	params := DriverParameters{}

	params.Zone, _ = parameters["zone"].(int)

	params.Bucket, ok = parameters["bucket"].(string)
	if !ok || params.Bucket == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
	}

	params.BaseURL, ok = parameters["baseurl"].(string)
	if !ok || params.BaseURL == "" {
		return nil, fmt.Errorf("No baseurl parameter provided")
	}

	params.Config.AccessKey, ok = parameters["accesskey"].(string)
	if !ok || params.Config.AccessKey == "" {
		return nil, fmt.Errorf("No accesskey parameter provided")
	}

	params.Config.SecretKey, ok = parameters["secretkey"].(string)
	if !ok || params.Config.SecretKey == "" {
		return nil, fmt.Errorf("No secretkey parameter provided")
	}

	params.RootDirectory, _ = parameters["rootdirectory"].(string)

	params.Config.RSHost, _ = parameters["rshost"].(string)
	params.Config.RSFHost, _ = parameters["rsfhost"].(string)
	params.Config.IoHost, _ = parameters["iohost"].(string)
	params.Config.UpHosts, _ = parameters["uphosts"].([]string)

	return New(params)
}

type baseEmbed struct {
	base.Base
}

type Driver struct {
	baseEmbed
}

func New(params DriverParameters) (*Driver, error) {

	client := kodo.New(params.Zone, &params.Config)
	bucket := client.Bucket(params.Bucket)

	params.RootDirectory = strings.TrimRight(params.RootDirectory, "/")

	if !strings.HasSuffix(params.BaseURL, "/") {
		params.BaseURL += "/"
	}

	d := &driver{
		params: params,
		client: client,
		bucket: &bucket,
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

type driver struct {
	params DriverParameters
	bucket *kodo.Bucket
	client *kodo.Client
}

// Name returns the human-readable "name" of the driver, useful in error
// messages and logging. By convention, this will just be the registration
// name, but drivers may provide other information here.
func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
// This should primarily be used for small objects.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {

	rc, err := d.ReadStream(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	body, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *driver) PutContent(ctx context.Context, path string, content []byte) error {

	err := d.bucket.Put(ctx, nil, d.getKey(path), bytes.NewBuffer(content), int64(len(content)), nil)
	return err
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *driver) ReadStream(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {

	stat, err := d.bucket.Stat(ctx, d.getKey(path))
	if err != nil {
		return nil, parseError(path, err)
	}

	if offset >= stat.Fsize {
		return ioutil.NopCloser(bytes.NewReader(nil)), nil
	}

	policy := kodo.GetPolicy{Expires: defaultExpiry}
	baseURL := d.params.BaseURL + d.getKey(path)
	url := d.client.MakePrivateUrl(baseURL, &policy)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		req.Header.Add("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	}

	// time.Sleep(15e9)

	resp, err := d.client.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	return resp.Body, nil
}

// WriteStream stores the contents of the provided io.ReadCloser at a
// location designated by the given path.
// May be used to resume writing a stream by providing a nonzero offset.
// The offset must be no larger than the CurrentSize for this path.
func (d *driver) WriteStream(ctx context.Context, path string, offset int64, reader io.Reader) (nn int64, err error) {

	uptoken := qiniuup.MakeAuthTokenString(d.client.AccessKey, d.client.SecretKey, &qiniuup.AuthPolicy{
		Scope:    d.bucket.Name + ":" + d.getKey(path),
		Deadline: 3600 + uint32(time.Now().Unix()),
		Accesses: []string{d.getKey(path)},
	})

	writeWholeFile := false

	pathNotFoundErr := storagedriver.PathNotFoundError{Path: path}

	stat, err := d.Stat(ctx, path)
	if err != nil {
		if err.Error() == pathNotFoundErr.Error() {
			writeWholeFile = true
		} else {
			return 0, err
		}
	}

	path = d.getKey(path)

	//write reader to local temp file
	tmpF, err := ioutil.TempFile("/tmp", "qiniu_driver")
	if err != nil {
		return 0, err
	}

	defer os.Remove(tmpF.Name())
	defer tmpF.Close()

	written, err := io.Copy(tmpF, reader)
	if err != nil {
		return 0, err
	}
	tmpF.Sync()
	_, err = tmpF.Seek(0, os.SEEK_SET)
	if err != nil {
		return 0, err
	}

	//------------------------

	if writeWholeFile == false {
		parts := make([]qiniurs.Part, 0)

		if offset == 0 {
			part_Reader := qiniurs.Part{
				FileName: "",
				R:        tmpF,
			}
			parts = append(parts, part_Reader)

			if written < stat.Size() {
				part_OriginFile2 := qiniurs.Part{
					Key:  path,
					From: written,
					To:   -1,
				}
				parts = append(parts, part_OriginFile2)
			}

		} else if offset == stat.Size() { //因为parts_api有闭区间写错了，故这里先特殊判断offset == stat.Size()
			part_OriginFile1 := qiniurs.Part{
				Key:  path,
				From: 0,
				To:   -1,
			}
			parts = append(parts, part_OriginFile1)

			part_Reader := qiniurs.Part{
				FileName: "",
				R:        tmpF,
			}
			parts = append(parts, part_Reader)
		} else if offset < stat.Size() {
			part_OriginFile1 := qiniurs.Part{
				Key:  path,
				From: 0,
				To:   offset,
			}
			parts = append(parts, part_OriginFile1)

			appendSize := written + offset
			part_Reader := qiniurs.Part{
				FileName: "",
				R:        tmpF,
			}
			parts = append(parts, part_Reader)

			if appendSize < stat.Size() {
				part_OriginFile2 := qiniurs.Part{
					Key:  path,
					From: appendSize,
					To:   -1,
				}
				parts = append(parts, part_OriginFile2)
			}
		} else if offset > stat.Size() {
			part_OriginFile1 := qiniurs.Part{
				Key:  path,
				From: 0,
				To:   -1,
			}
			parts = append(parts, part_OriginFile1)

			zeroBytes := make([]byte, offset-stat.Size())
			part_ZeroPart := qiniurs.Part{
				R: bytes.NewReader(zeroBytes),
			}
			parts = append(parts, part_ZeroPart)

			part_Reader := qiniurs.Part{
				R: tmpF,
			}
			parts = append(parts, part_Reader)
		}
		err = qiniurs.PutParts(nil, nil, uptoken, path, true, parts, nil)
		if err != nil {
			return 0, err
		}
	} else {
		err := d.bucket.PutFile(ctx, nil, path, tmpF.Name(), nil)
		if err != nil {
			return 0, err
		}
	}

	return written, nil
}

// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {

	items, _, _, err := d.bucket.List(ctx, d.getKey(path), "", "", 1)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
		err = nil
	}

	if len(items) == 0 {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	item := items[0]

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if d.getKey(path) != item.Key {
		fi.IsDir = true
	}

	if !fi.IsDir {
		fi.Size = item.Fsize
		fi.ModTime = time.Unix(0, item.PutTime*100)
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the
// given path.
func (d *driver) List(ctx context.Context, path string) ([]string, error) {

	if path != "/" && path[len(path)-1] != '/' {
		path += "/"
	}

	// This is to cover for the cases when the rootDirectory of the driver is either "" or "/".
	// In those cases, there is no root prefix to replace and we must actually add a "/" to all
	// results in order to keep them as valid paths as recognized by storagedriver.PathRegexp
	rootPrefix := ""
	if d.getKey("") == "" {
		rootPrefix = "/"
	}

	var (
		items    []kodo.ListItem
		marker   string
		prefixes []string
		err      error

		files       []string
		directories []string
	)

	for {
		items, prefixes, marker, err = d.bucket.List(ctx, d.getKey(path), "/", marker, listMax)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			err = nil
		}

		for _, item := range items {
			files = append(files, strings.Replace(item.Key, d.getKey(""), rootPrefix, 1))
		}

		for _, prefix := range prefixes {
			directories = append(directories, strings.Replace(strings.TrimSuffix(prefix, "/"), d.getKey(""), rootPrefix, 1))
		}

		if marker == "" {
			break
		}
	}

	return append(files, directories...), nil
}

// Move moves an object stored at sourcePath to destPath, removing the
// original object.
// Note: This may be no more efficient than a copy followed by a delete for
// many implementations.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {

	_, err := d.bucket.Stat(ctx, d.getKey(sourcePath))
	if err != nil {
		return parseError(sourcePath, err)
	}

	err = d.bucket.Delete(ctx, d.getKey(destPath))
	if err != nil {
		if !isKeyNotExists(err) {
			return err
		}
	}

	err = d.bucket.Move(ctx, d.getKey(sourcePath), d.getKey(destPath))
	return parseError(sourcePath, err)
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {

	var (
		items  []kodo.ListItem
		marker string
		err    error

		cnt int
	)

	for {
		items, _, marker, err = d.bucket.List(ctx, d.getKey(path), "", marker, listMax)
		if err != nil {
			if err != io.EOF {
				return err
			}
			err = nil
		}

		cnt += len(items)
		if cnt == 0 {
			return storagedriver.PathNotFoundError{Path: path}
		}

		for _, item := range items {
			err = d.bucket.Delete(ctx, item.Key)
			if err != nil {
				if isKeyNotExists(err) {
					continue
				}
				return err
			}
		}

		if marker == "" {
			break
		}
	}

	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at
// the given path, possibly using the given options.
// May return an ErrUnsupportedMethod in certain StorageDriver
// implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {

	policy := kodo.GetPolicy{Expires: defaultExpiry}

	if expiresTime, ok := options["expiry"].(time.Time); ok {
		if expires := expiresTime.Unix() - time.Now().Unix(); expires > 0 {
			policy.Expires = uint32(expires)
		}
	}

	baseURL := d.params.BaseURL + d.getKey(path)
	url := d.client.MakePrivateUrl(baseURL, &policy)
	return url, nil
}

func (d *driver) getKey(path string) string {
	return strings.TrimLeft(d.params.RootDirectory+path, "/")
}

func isKeyNotExists(err error) bool {
	if er, ok := err.(*rpc.ErrorInfo); ok && er.Code == 612 {
		return true
	}
	return false
}

func parseError(path string, err error) error {
	if er, ok := err.(*rpc.ErrorInfo); ok && er.Code == 612 {
		return storagedriver.PathNotFoundError{Path: path}
	}
	return err
}
