package us3

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	uerr "github.com/ucloud/ucloud-sdk-go/ucloud/error"
	us3 "github.com/ufilesdk-dev/ufile-gosdk"

	"github.com/beyondstorage/go-endpoint"
	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/pkg/credential"
	"github.com/beyondstorage/go-storage/v4/services"
	"github.com/beyondstorage/go-storage/v4/types"
)

type Service struct {
	service *us3.UFileRequest

	defaultPairs DefaultServicePairs
	features     ServiceFeatures

	types.UnimplementedServicer
}

func (s *Service) String() string {
	return fmt.Sprintf("Servicer us3")
}

// Storage is the example client.
type Storage struct {
	client *us3.UFileRequest

	bucket  string
	workDir string

	defaultPairs DefaultStoragePairs
	features     StorageFeatures

	types.UnimplementedStorager
}

// String implements Storager.String
func (s *Storage) String() string {
	return fmt.Sprintf("Storager us3 {Name: %s, WorkDir: %s", s.bucket, s.workDir)
}

func New(pairs ...types.Pair) (types.Servicer, types.Storager, error) {
	return newServicerAndStorager(pairs...)
}

func NewServicer(pairs ...types.Pair) (types.Servicer, error) {
	return newServicer(pairs...)
}

// NewStorager will create Storager only.
func NewStorager(pairs ...types.Pair) (types.Storager, error) {
	_, store, err := newServicerAndStorager(pairs...)
	return store, err
}

func newServicer(pairs ...types.Pair) (srv *Service, err error) {
	defer func() {
		if err != nil {
			err = services.InitError{
				Op:    "new_servicer",
				Type:  Type,
				Err:   formatError(err),
				Pairs: pairs,
			}
		}
	}()

	srv = &Service{}

	opt, err := parsePairServiceNew(pairs)
	if err != nil {
		return nil, err
	}

	cp, err := credential.Parse(opt.Credential)
	if err != nil {
		return nil, err
	}
	if cp.Protocol() != credential.ProtocolHmac {
		return nil, services.PairUnsupportedError{Pair: ps.WithCredential(opt.Credential)}
	}
	ak, sk := cp.Hmac()

	ep, err := endpoint.Parse(opt.Endpoint)
	if err != nil {
		return nil, err
	}

	var url string
	switch ep.Protocol() {
	case endpoint.ProtocolHTTP:
		url, _, _ = ep.HTTP()
	case endpoint.ProtocolHTTPS:
		url, _, _ = ep.HTTPS()
	default:
		return nil, services.PairUnsupportedError{Pair: ps.WithEndpoint(opt.Endpoint)}
	}

	config := &us3.Config{
		PublicKey:  ak,
		PrivateKey: sk,
		FileHost:   url,
		BucketHost: "api.ucloud.cn",
	}

	srv.service, err = us3.NewFileRequest(config, nil)
	if err != nil {
		return nil, err
	}

	if opt.HasDefaultServicePairs {
		srv.defaultPairs = opt.DefaultServicePairs
	}
	if opt.HasServiceFeatures {
		srv.features = opt.ServiceFeatures
	}

	return
}

func newServicerAndStorager(pairs ...types.Pair) (srv *Service, store *Storage, err error) {
	srv, err = newServicer(pairs...)
	if err != nil {
		return nil, nil, err
	}

	store, err = srv.newStorage(pairs...)
	if err != nil {
		err = services.InitError{Op: "new_storager", Type: Type, Err: formatError(err), Pairs: pairs}
		return nil, nil, err
	}

	return
}

func (s *Service) newStorage(pairs ...types.Pair) (store *Storage, err error) {
	opt, err := parsePairStorageNew(pairs)
	if err != nil {
		return nil, err
	}

	store = &Storage{
		client:  s.service,
		bucket:  opt.Name,
		workDir: "/",
	}

	if opt.HasWorkDir {
		store.workDir = opt.WorkDir
	}
	if opt.HasStorageFeatures {
		store.features = opt.StorageFeatures
	}
	if opt.HasDefaultStoragePairs {
		store.defaultPairs = opt.DefaultStoragePairs
	}

	return
}

func (s *Service) formatError(op string, err error, name string) error {
	if err == nil {
		return nil
	}

	return services.ServiceError{
		Op:       op,
		Err:      formatError(err),
		Servicer: s,
		Name:     name,
	}
}

func (s *Storage) formatError(op string, err error, path ...string) error {
	if err == nil {
		return nil
	}

	return services.StorageError{
		Op:       op,
		Err:      formatError(err),
		Storager: s,
		Path:     path,
	}
}

const (
	// UCloud us3 RetCode
	AccessDenied = -148643
	NoSuchKey    = -148654
)

// formatError converts errors returned by SDK into errors defined in go-storage and go-service-*.
// The original error SHOULD NOT be wrapped.
func formatError(err error) error {
	if _, ok := err.(services.InternalError); ok {
		return err
	}

	e, ok := err.(*uerr.ServerError)
	if ok {
		switch e.Code() {
		case AccessDenied:
			return fmt.Errorf("%w, %v", services.ErrPermissionDenied, err)
		case NoSuchKey:
			return fmt.Errorf("%w, %v", services.ErrObjectNotExist, err)
		default:
			return fmt.Errorf("%w, %v", services.ErrUnexpected, err)
		}
	}

	return fmt.Errorf("%w, %v", services.ErrUnexpected, err)
}

func (s *Storage) getAbsPath(path string) string {
	prefix := strings.TrimPrefix(s.workDir, "/")
	return prefix + path
}

func (s *Storage) getRelPath(path string) string {
	prefix := strings.TrimPrefix(s.workDir, "/")
	return strings.TrimPrefix(path, prefix)
}

func (s *Storage) formatFileObject(v us3.ObjectInfo) (o *types.Object, err error) {
	o = s.newObject(false)
	o.ID = v.Key
	o.Path = s.getRelPath(v.Key)
	o.Mode |= types.ModeRead

	length, err := strconv.ParseInt(v.Size, 10, 64)
	if err != nil {
		return nil, err
	}
	o.SetContentLength(length)
	// The LastModified return value of UCloud us3 feedback is a timestamp.
	lastModified := time.Unix(int64(v.LastModified)/1000, 0)
	o.SetLastModified(lastModified)

	if v.Etag != "" {
		o.SetEtag(v.Etag)
	}

	var sm ObjectSystemMetadata
	if value := v.StorageClass; value != "" {
		sm.StorageClass = v.StorageClass
	}
	o.SetSystemMetadata(sm)

	return
}

func (s *Storage) newObject(done bool) *types.Object {
	return types.NewObject(s, done)
}
