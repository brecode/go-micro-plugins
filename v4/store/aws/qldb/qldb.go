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
	driver  *qldbdriver.QLDBDriver

	cfg *Config
	ctx context.Context
}

func init() {
	cmd.DefaultStores["qldb"] = NewStore
}

func (q *QLDB) Init(opts ...store.Option) error {
	for _, o := range opts {
		o(&q.options)
	}

	// best-effort configure the store
	if err := q.configure(); err != nil {
		return err
	}

	return nil
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
	r, err := q.driver.Execute(q.ctx, func(txn qldbdriver.Transaction) (interface{}, error) {
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
	st := fmt.Sprintf("INSERT INTO %s ?", q.options.Table)
	idempotentStmt := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", q.options.Table, r.Key)

	_, err := q.driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {

		// This is critical to make this transaction idempotent
		result, err := txn.Execute(idempotentStmt, r.Key)
		if err != nil {
			return nil, err
		}

		// Check if there are any results, if not write to ledger
		if result.Next(txn) {
			return nil, errors.New("document already exists") // ignore document
		} else {

			var t interface{}
			json.Unmarshal(r.Value, &t)
			if err != nil {
				return nil, err
			}

			_, err = txn.Execute(st, t)
			if err != nil {
				return nil, err
			}
		}

		return nil, nil
	})

	return err

}

func (q *QLDB) Delete(key string, opts ...store.DeleteOption) error {
	q.options.Logger.Log(logger.WarnLevel, "delete not allowed")
	return nil
}

func (q *QLDB) List(opts ...store.ListOption) ([]string, error) {
	q.options.Logger.Log(logger.InfoLevel, "list not implemented")
	return nil, nil
}

func (q *QLDB) Close() error {
	q.options.Logger.Log(logger.InfoLevel, "closing")

	q.driver.Shutdown(q.options.Context)
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

	// new store
	s := new(QLDB)
	// set the options
	s.options = *options
	s.cfg = cfg
	s.ctx = options.Context

	// return store
	return s
}

// configure instantiates the driver to access QLDB
func (q *QLDB) configure() error {
	var err error

	// cleanup driver
	if q.driver != nil {
		q.driver.Shutdown(context.TODO())
	}

	// role based access / iam
	cfg := aws.Config{
		Region: q.cfg.Region,
		Credentials: credentials.NewStaticCredentialsProvider(
			q.cfg.APIKey,
			q.cfg.APISecret,
			""),
	}

	session := qldbsession.NewFromConfig(cfg, func(options *qldbsession.Options) {})

	q.driver, err = qldbdriver.New(q.options.Database,
		session,
		func(options *qldbdriver.DriverOptions) {
			options.LoggerVerbosity = qldbdriver.LogInfo
		})
	_, err = q.driver.GetTableNames(q.ctx)

	return err
}
