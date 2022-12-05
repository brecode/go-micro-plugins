// Package qldb implements the aws qldb ledger store
package qldb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/qldbsession"
	"github.com/awslabs/amazon-qldb-driver-go/v3/qldbdriver"
	"go-micro.dev/v4/logger"
	"go-micro.dev/v4/store"
	"go-micro.dev/v4/util/cmd"
)

type Config struct {
	APIKey     string
	APISecret  string
	Region     string
	MaxRetries int
}

type QLDB struct {
	options store.Options

	session *qldbsession.Client
	driver  *qldbdriver.QLDBDriver
	qldbdriver.QLDBDriver

	cfg *Config
}

func init() {
	cmd.DefaultStores["qldb"] = NewStore
}

func (q *QLDB) Init(opts ...store.Option) error {
	for _, o := range opts {
		o(&q.options)
	}
	/// re-configure for options
	return q.configure()
}

func (q *QLDB) Options() store.Options {
	return q.options
}

func (q *QLDB) Read(key string, opts ...store.ReadOption) ([]*store.Record, error) {

	var options store.ReadOptions
	var records []*store.Record

	for _, o := range opts {
		o(&options)
	}

	st := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", q.options.Table, key)
	r, err := q.driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute(st, key)
		if err != nil {
			return nil, err
		}

		for result.Next(txn) {
			ionBinary := result.GetCurrentData()

			v := &store.Record{
				Value: ionBinary,
			}
			records = append(records, v)
		}
		if result.Err() != nil {
			return nil, result.Err()
		}

		return records, nil
	})
	if err != nil {
		return nil, err
	}

	return r.([]*store.Record), nil
}

func (q *QLDB) Write(r *store.Record, opts ...store.WriteOption) error {

	// statements for transactions
	stmt := fmt.Sprintf("INSERT INTO %s ?", q.options.Table)
	idempotentStmt := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", q.options.Table, r.Key)

	_, err := q.driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {

		// This is critical to make this transaction idempotent
		result, err := txn.Execute(idempotentStmt, r.Key)
		if err != nil {
			return nil, err
		}

		// Check if there are any results, if not write to ledger
		if result.Next(txn) {
			return nil, errors.New("document already exists")
		} else {

			var t interface{}
			json.Unmarshal(r.Value, &t)

			if err != nil {
				return nil, err
			}

			_, err = txn.Execute(stmt, t)
			if err != nil {
				return nil, err
			}
		}

		return nil, nil
	})

	return err

}

func (q *QLDB) Delete(key string, opts ...store.DeleteOption) error {
	return nil
}

func (q *QLDB) List(opts ...store.ListOption) ([]string, error) {
	if logger.V(logger.DebugLevel, logger.DefaultLogger) {
		logger.Info("Not implemented yet")
	}
	return nil, nil
}

func (q *QLDB) Close() error {
	q.driver.Shutdown(context.TODO())
	return nil
}

func (q *QLDB) String() string {
	return "qldb"
}

// NewStore returns a new Store backed by qldb
func NewStore(opts ...store.Option) store.Store {
	options := &store.Options{}

	for _, o := range opts {
		o(options)
	}

	// get index key for optimized reads
	cfg, _ := options.Context.Value(struct{}{}).(*Config)
	logger.Log(logger.DebugLevel, "QLDB config: %v", cfg)

	// new store
	s := new(QLDB)
	// set the options
	s.options = *options
	s.cfg = cfg

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
	var err error

	cfg := aws.Config{
		Region: q.cfg.Region,
		Credentials: credentials.NewStaticCredentialsProvider(
			q.cfg.APIKey,
			q.cfg.APISecret,
			""),
	}

	q.session = qldbsession.NewFromConfig(cfg, func(options *qldbsession.Options) {})

	if q.driver != nil {
		q.driver.Shutdown(context.TODO())
	}

	q.driver, err = qldbdriver.New(q.options.Database,
		q.session,
		func(options *qldbdriver.DriverOptions) {
			options.LoggerVerbosity = qldbdriver.LogInfo
		})

	return err
}
