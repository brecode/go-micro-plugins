// Package qldb implements the aws qldb ledger store
package qldb

import (
	"context"
	"fmt"
	"regexp"

	"github.com/golang/protobuf/proto"

	"github.com/amzn/ion-go/ion"

	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/aws/aws-sdk-go-v2/service/qldbsession"
	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver"
	qldbpb "github.com/go-micro/plugins/v4/store/aws/qldb/proto"
	"go-micro.dev/v4/logger"
	"go-micro.dev/v4/store"
	"go-micro.dev/v4/util/cmd"
)

// DefaultJournal is the journal that qldb
// will use if no other journal name is provided.
var (
	DefaultLedger = "qldb-test"
	DefaultTable  = "Person"
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
	session *qldbsession.Client
	driver  *qldbdriver.QLDBDriver
}

func init() {
	cmd.DefaultStores["qldb"] = NewStore
}

func (q *QLDB) Init(opts ...store.Option) error {
	for _, o := range opts {
		o(&q.options)
	}

	return q.configure()
}

func (q *QLDB) Options() store.Options {
	//TODO implement me
	panic("implement me")
}

func (q *QLDB) Read(key string, opts ...store.ReadOption) ([]*store.Record, error) {

	p, err := q.driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute("SELECT * FROM Person WHERE TestFieldOne = ?", key)
		if err != nil {
			return nil, err
		}

		// Assume the result is not empty
		hasNext := result.Next(txn)
		if !hasNext && result.Err() != nil {
			return nil, result.Err()
		}

		ionBinary := result.GetCurrentData()

		temp := &qldbpb.Record{}
		err = ion.Unmarshal(ionBinary, temp)
		if err != nil {
			return nil, err
		}

		return temp, nil
	})
	if err != nil {
		panic(err)
	}

	b, _ := proto.Marshal(p.(*qldbpb.Record))
	arr := []*store.Record{
		{
			Value: b,
		},
	}

	return arr, nil
}

func (q *QLDB) Write(r *store.Record, opts ...store.WriteOption) error {

	stmt := fmt.Sprintf("INSERT INTO %s ?", r.Key)

	temp := &qldbpb.Record{}
	err := proto.Unmarshal(r.Value, temp)

	if err != nil {
		panic(err)
	}

	_, err = q.driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		return txn.Execute(stmt, temp)
	})
	if err != nil {
		panic(err)
	}

	return nil

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
	q.driver.Shutdown(context.TODO())
	return nil
}

func (q *QLDB) String() string {
	//TODO implement me
	panic("implement me")
}

// NewStore returns a new Store backed by qldb
func NewStore(opts ...store.Option) store.Store {
	options := store.Options{
		Database: DefaultLedger,
		Table:    DefaultTable,
	}

	for _, o := range opts {
		o(&options)
	}

	// new store
	s := new(QLDB)
	// set the options
	s.options = options

	// best-effort configure the store
	if err := s.configure(); err != nil {
		if logger.V(logger.ErrorLevel, logger.DefaultLogger) {
			logger.Error("Error configuring store ", err)
		}
	}

	// return store
	return s
}

func (q *QLDB) configure() error {

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return err
	}

	q.session = qldbsession.NewFromConfig(cfg, func(options *qldbsession.Options) {
		options.Region = "us-west-2"
	})

	if q.driver != nil {
		q.driver.Shutdown(context.TODO())
	}

	q.driver, err = qldbdriver.New(DefaultLedger,
		q.session,
		func(options *qldbdriver.DriverOptions) {
			options.LoggerVerbosity = qldbdriver.LogInfo
		})

	return err
}
