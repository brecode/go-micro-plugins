// Package qldb implements the aws qldb ledger store
package qldb

import (
	"github.com/aws/aws-sdk-go-v2/config"
	"regexp"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver"

	"go-micro.dev/v4/logger"

	"go-micro.dev/v4/store"
	"go-micro.dev/v4/util/cmd"
)

// DefaultJournal is the journal that qldb
// will use if no other journal name is provided.
var (
	DefaultLedger = "amber"
	DefaultTable  = "user-data"
)

var (
	re = regexp.MustCompile("[^a-zA-Z0-9]+")

	statements = map[string]string{
		"list":       "SELECT key, value, metadata, expiry FROM %s.%s;",
		"read":       "SELECT key, value, metadata, expiry FROM %s.%s WHERE key = $1;",
		"readMany":   "SELECT key, value, metadata, expiry FROM %s.%s WHERE key LIKE $1;",
		"readOffset": "SELECT key, value, metadata, expiry FROM %s.%s WHERE key LIKE $1 ORDER BY key DESC LIMIT $2 OFFSET $3;",
		"write":      "INSERT INTO %s.%s(key, value, metadata, expiry) VALUES ($1, $2::bytea, $3, $4) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, metadata = EXCLUDED.metadata, expiry = EXCLUDED.expiry;",
		"delete":     "DELETE FROM %s.%s WHERE key = $1;",
	}
)

type QLDB struct {
	options store.Options
	session *qldbsession.QLDBSession
	driver  *qldbdriver.QLDBDriver

	sync.RWMutex
	// known databases
	databases map[string]bool
}

func init() {
	cmd.DefaultStores["qldb"] = NewStore
}

func (q *QLDB) Init(option ...store.Option) error {
	var err error
	awsSession := session.Must(session.NewSession(aws.NewConfig().WithRegion("us-east-1")))
	q.session = qldbsession.New(awsSession)

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return err
	}

	qldbSession := qldbsession.NewFromConfig(cfg, func(options *qldbsession.Options) {
		options.Region = "us-east-1"}

	q.driver, err = qldbdriver.New("quick-start",
		q.session,
		func(options *qldbdriver.DriverOptions) {
			options.LoggerVerbosity = qldbdriver.LogInfo
		}))
	return err
}

func (q *QLDB) Options() store.Options {
	//TODO implement me
	panic("implement me")
}

func (q *QLDB) Read(key string, opts ...store.ReadOption) ([]*store.Record, error) {
	//TODO implement me
	panic("implement me")
}

func (q *QLDB) Write(r *store.Record, opts ...store.WriteOption) error {
	//TODO implement me
	panic("implement me")
}

func (q *QLDB) Delete(key string, opts ...store.DeleteOption) error {
	//TODO implement me
	panic("implement me")
}

func (q *QLDB) List(opts ...store.ListOption) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (q *QLDB) Close() error {
	//TODO implement me
	panic("implement me")
}

func (q *QLDB) String() string {
	//TODO implement me
	panic("implement me")
}

// NewStore returns a new Store backed by qldb
func NewStore(opts ...store.Option) store.Store {
	options := store.Options{
		Database: DefaultDatabase,
		Table:    DefaultTable,
	}

	for _, o := range opts {
		o(&options)
	}

	// new store
	s := new(QLDB)
	// set the options
	s.options = options
	// mark known databases
	s.databases = make(map[string]bool)
	// best-effort configure the store
	if err := s.configure(); err != nil {
		if logger.V(logger.ErrorLevel, logger.DefaultLogger) {
			logger.Error("Error configuring store ", err)
		}
	}

	// return store
	return s
}
