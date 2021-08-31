package us3

import (
	"context"
	"io"
	"strconv"
	"time"

	uerr "github.com/ucloud/ucloud-sdk-go/ucloud/error"

	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/pkg/iowrap"
	"github.com/beyondstorage/go-storage/v4/services"
	. "github.com/beyondstorage/go-storage/v4/types"
)

func (s *Storage) create(path string, opt pairStorageCreate) (o *Object) {
	rp := s.getAbsPath(path)

	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		if !s.features.VirtualDir {
			return
		}

		rp += "/"
		o = s.newObject(true)
		o.Mode |= ModeDir
	} else {
		o = s.newObject(false)
		o.Mode |= ModeRead
	}

	o.ID = rp
	o.Path = path

	return o
}

func (s *Storage) delete(ctx context.Context, path string, opt pairStorageDelete) (err error) {
	rp := s.getAbsPath(path)

	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		if !s.features.VirtualDir {
			err = services.PairUnsupportedError{Pair: ps.WithObjectMode(opt.ObjectMode)}
			return err
		}

		rp += "/"
	}

	err = s.client.DeleteFile(rp)
	if err != nil {
		if e, ok := err.(uerr.ServerError); ok && e.Code() == NoSuchKey {
			// us3 DeleteFile is not idempotent, so we need to check file_not_exists error.
			//
			// - [GSP-46](https://github.com/beyondstorage/specs/blob/master/rfcs/46-idempotent-delete.md)
			// - https://ucloud-us3.github.io/go-sdk/%E6%96%87%E4%BB%B6%E5%88%A0%E9%99%A4.html
			err = nil
		} else {
			return err
		}
	}

	return nil
}

func (s *Storage) list(ctx context.Context, path string, opt pairStorageList) (oi *ObjectIterator, err error) {
	input := &objectPageStatus{
		maxKeys: 200,
		prefix:  s.getAbsPath(path),
	}

	if !opt.HasListMode {
		// Support `ListModePrefix` as the default `ListMode`.
		// ref: [GSP-46](https://github.com/beyondstorage/go-storage/blob/master/docs/rfcs/654-unify-list-behavior.md)
		opt.ListMode = ListModePrefix
	}

	var nextFn NextObjectFunc

	switch {
	case opt.ListMode.IsDir():
		input.delimiter = "/"
		nextFn = s.nextObjectPageByDir
	case opt.ListMode.IsPrefix():
		nextFn = s.nextObjectPageByPrefix
	default:
		return nil, services.ListModeInvalidError{Actual: opt.ListMode}
	}

	return NewObjectIterator(ctx, nextFn, input), nil
}

func (s *Storage) metadata(opt pairStorageMetadata) (meta *StorageMeta) {
	meta = NewStorageMeta()
	meta.Name = s.bucket
	meta.WorkDir = s.workDir
	return meta
}

func (s *Storage) nextObjectPageByDir(ctx context.Context, page *ObjectPage) error {
	input := page.Status.(*objectPageStatus)

	output, err := s.client.ListObjects(input.prefix, input.marker, input.delimiter, input.maxKeys)
	if err != nil {
		return err
	}

	for _, v := range output.CommonPrefixes {
		o := s.newObject(true)
		o.ID = v.Prefix
		o.Path = s.getRelPath(v.Prefix)
		o.Mode |= ModeDir

		page.Data = append(page.Data, o)
	}

	for _, v := range output.Contents {
		o, err := s.formatFileObject(v)
		if err != nil {
			return err
		}

		page.Data = append(page.Data, o)
	}
	if output.NextMarker == "" {
		return IterateDone
	}
	if !output.IsTruncated {
		return IterateDone
	}
	input.marker = output.NextMarker

	return err
}

func (s *Storage) nextObjectPageByPrefix(ctx context.Context, page *ObjectPage) error {
	input := page.Status.(*objectPageStatus)

	output, err := s.client.ListObjects(input.prefix, input.marker, input.delimiter, input.maxKeys)
	if err != nil {
		return err
	}

	for _, v := range output.Contents {
		o, err := s.formatFileObject(v)
		if err != nil {
			return err
		}

		page.Data = append(page.Data, o)
	}

	if output.NextMarker == "" {
		return IterateDone
	}
	if !output.IsTruncated {
		return IterateDone
	}

	input.marker = output.NextMarker

	return err
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt pairStorageRead) (n int64, err error) {
	rp := s.getAbsPath(path)

	reqUrl := s.client.GetPrivateURL(rp, 3600*time.Second)
	err = s.client.Download(reqUrl)
	if err != nil {
		return 0, err
	}

	var rc io.ReadCloser
	rc = iowrap.CallbackReadCloser(rc, func(bytes []byte) {
		bytes = s.client.LastResponseBody
	})

	if opt.HasIoCallback {
		rc = iowrap.CallbackReadCloser(rc, opt.IoCallback)
	}

	return io.Copy(w, rc)
}

func (s *Storage) stat(ctx context.Context, path string, opt pairStorageStat) (o *Object, err error) {
	rp := s.getAbsPath(path)

	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		if !s.features.VirtualDir {
			err = services.PairUnsupportedError{Pair: ps.WithObjectMode(opt.ObjectMode)}
			return nil, err
		}

		rp += "/"
	}

	err = s.client.HeadFile(rp)
	if err != nil {
		return nil, err
	}

	o = s.newObject(true)
	o.ID = rp
	o.Path = path

	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		o.Mode |= ModeDir
	} else {
		o.Mode |= ModeRead
	}

	output := s.client.LastResponseHeader

	var value string

	if value = output.Get("Content-Length"); value != "" {
		length, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, err
		}

		o.SetContentLength(length)
	}
	if value = output.Get("Last-Modified"); value != "" {
		// UCloud us3 feedback Last-Modified format is:
		// Mon, 02 Jan 2006 15:04:05 GMT
		lastModified, err := time.Parse(time.RFC1123, value)
		if err != nil {
			return nil, err
		}

		o.SetLastModified(lastModified)
	}
	if value = output.Get("Content-Type"); value != "" {
		o.SetContentType(value)
	}
	if value = output.Get("ETag"); value != "" {
		o.SetEtag(value)
	}

	var sm ObjectSystemMetadata
	if v := output.Get("X-Ufile-Storage-Class"); v != "" {
		sm.StorageClass = v
	}

	o.SetSystemMetadata(sm)

	return
}

func (s *Storage) write(ctx context.Context, path string, r io.Reader, size int64, opt pairStorageWrite) (n int64, err error) {
	rp := s.getAbsPath(path)

	r = io.LimitReader(r, size)

	if opt.HasIoCallback {
		r = iowrap.CallbackReader(r, opt.IoCallback)
	}

	err = s.client.IOPut(r, rp, "")
	if err != nil {
		return 0, err
	}

	return size, nil
}
