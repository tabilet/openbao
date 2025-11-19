// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

// Package raft implements the Raft physical storage backend for OpenBao.
//
// LOCK DESIGN:
// This package uses a three-level lock hierarchy to ensure correctness and performance.
// For complete documentation on the locking strategy, see LOCKS.md in this directory.
//
// Quick Reference:
//   - FSM.l (sync.RWMutex): Protects FSM structure and database cache
//   - RLock: Used for all read operations (high concurrency)
//   - Lock: Used for structure modifications (exclusive)
//   - Always acquire in order: semaphore → FSM lock → BoltDB transaction
//
// Key Patterns:
//   - getDB(): Double-check locking for optimal cache performance
//   - withDBView/withDBUpdate: Helper functions encapsulating lock + database access
//   - ApplyBatch: Coarse-grained locking for batch consistency
package raft

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/hashicorp/go-hclog"
	metrics "github.com/hashicorp/go-metrics/compat"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/go-raftchunking"
	"github.com/hashicorp/go-secure-stdlib/strutil"
	"github.com/hashicorp/raft"
	"github.com/openbao/openbao/sdk/v2/helper/jsonutil"
	"github.com/openbao/openbao/sdk/v2/physical"
	"github.com/openbao/openbao/sdk/v2/plugin/pb"
	"github.com/rboyer/safeio"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
)

const (
	deleteOp uint32 = 1 << iota
	putOp
	restoreCallbackOp
	getOp
	verifyReadOp
	verifyListOp
	beginTxOp
	commitTxOp

	chunkingPrefix       = "raftchunking/"
	databaseFilenameBase = "vault"
	databaseFilenameExt  = ".db"

	// File permissions for database files
	dbFilePermissions      = 0o600
	unwantedPermissionBits = 0o077
)

var (
	// dataBucketName is the value we use for the bucket
	dataBucketName     = []byte("data")
	configBucketName   = []byte("config")
	latestIndexKey     = []byte("latest_indexes")
	latestConfigKey    = []byte("latest_config")
	localNodeConfigKey = []byte("local_node_config")
)

// Verify FSM satisfies the correct interfaces
var (
	_ physical.Backend = (*FSM)(nil)
	_ raft.FSM         = (*FSM)(nil)
	_ raft.BatchingFSM = (*FSM)(nil)
)

type restoreCallback func(context.Context) error

// fsmEntryTxErrorKey is the value for a FSMEntry to signal that it is
// not the result of a regular Get operation (which cannot occur in a
// transaction) but is instead contains the response value from failing to
// apply this transaction.
const fsmEntryTxErrorKey = "[\ttransaction-commit-failure\t]"

type FSMEntry struct {
	Key   string
	Value []byte
}

func (f *FSMEntry) String() string {
	return fmt.Sprintf("Key: %s. Value: %s", f.Key, hex.EncodeToString(f.Value))
}

func (f *FSMEntry) IsTxError() bool {
	return f.Key == fsmEntryTxErrorKey
}

func (f *FSMEntry) AsTxError() error {
	str := string(f.Value)
	commitErr := physical.ErrTransactionCommitFailure.Error()

	split := strings.SplitN(str, commitErr, 2)
	if len(split) != 2 {
		return errors.New(str)
	}

	return fmt.Errorf("%v%w%v", split[0], physical.ErrTransactionCommitFailure, split[1])
}

// FSMApplyResponse is returned from an FSM apply. It indicates if the apply was
// successful or not. EntryMap contains the keys/values from the Get operations.
type FSMApplyResponse struct {
	Success    bool
	EntrySlice []*FSMEntry
}

// dbCacheEntry wraps a database handle with expiration information
type dbCacheEntry struct {
	db        *bolt.DB
	expiresAt time.Time
	noExpire  bool // true for default database
}

// FSM is Vault's primary state storage. It writes updates to a bolt db file
// that lives on local disk. FSM implements raft.FSM and physical.Backend
// interfaces.
type FSM struct {
	// latestIndex and latestTerm must stay at the top of this struct to be
	// properly 64-bit aligned.

	// latestIndex and latestTerm are the term and index of the last log we
	// received
	latestIndex *uint64
	latestTerm  *uint64
	// latestConfig is the latest server configuration we've seen
	latestConfig atomic.Value

	l           sync.RWMutex
	path        string
	logger      log.Logger
	noopRestore bool

	// applyCallback is used to control the pace of applies in tests
	applyCallback func()

	// Database cache using sync.Map for lock-free reads
	dbCache sync.Map // map[string]*dbCacheEntry

	// singleflight group prevents duplicate database opens
	dbOpenGroup singleflight.Group

	// Expiration configuration
	namespaceDBExpiration time.Duration
	expirationCheckPeriod time.Duration
	stopExpiration        chan struct{}

	// retoreCb is called after we've restored a snapshot
	restoreCb restoreCallback

	chunker *raftchunking.ChunkingBatchingFSM

	localID         string
	desiredSuffrage string
	unknownOpTypes  sync.Map

	// tracker for fast application of transactions
	fastTxnTracker *fsmTxnCommitIndexTracker

	mEnabled       bool
	invalidateHook physical.InvalidateFunc
}

// NewFSM constructs a FSM using the given directory
func NewFSM(path string, localID string, mEnabled bool, expire, cleanup time.Duration, logger log.Logger) (*FSM, error) {
	// Initialize the latest term, index, and config values
	latestTerm := new(uint64)
	latestIndex := new(uint64)
	latestConfig := atomic.Value{}
	atomic.StoreUint64(latestTerm, 0)
	atomic.StoreUint64(latestIndex, 0)
	latestConfig.Store((*ConfigurationValue)(nil))

	f := &FSM{
		path:   path,
		logger: logger,

		latestTerm:   latestTerm,
		latestIndex:  latestIndex,
		latestConfig: latestConfig,
		// Assume that the default intent is to join as as voter. This will be updated
		// when this node joins a cluster with a different suffrage, or during cluster
		// setup if this is already part of a cluster with a desired suffrage.
		desiredSuffrage: "voter",
		localID:         localID,
		fastTxnTracker:  FsmTxnCommitIndexTracker(),

		mEnabled: mEnabled,
	}

	f.chunker = raftchunking.NewChunkingBatchingFSM(f, &FSMChunkStorage{
		f:   f,
		ctx: context.Background(),
	})

	// Initialize expiration configuration
	f.namespaceDBExpiration = expire
	f.expirationCheckPeriod = cleanup
	f.stopExpiration = make(chan struct{})

	// Open the default database
	_, err := f.getDB(databaseFilename())
	if err != nil {
		return nil, err
	}

	// Start background expiration goroutine
	go f.runExpirationCleanup()

	return f, nil
}

func (f *FSM) getDatabaseName(ctx context.Context) (string, error) {
	if !f.mEnabled {
		return databaseFilename(), nil
	}
	return getDatabaseName(ctx)
}

// getDatabaseForContext retrieves the appropriate database for the given context.
// This combines database name resolution and database retrieval into a single operation.
func (f *FSM) getDatabaseForContext(ctx context.Context) (*bolt.DB, error) {
	vaultDBName, err := f.getDatabaseName(ctx)
	if err != nil {
		return nil, err
	}
	return f.getDB(vaultDBName)
}

// withDBView executes a read-only function against the database for the given context.
// It handles database retrieval automatically. No FSM locks needed - sync.Map is lock-free!
func (f *FSM) withDBView(ctx context.Context, fn func(*bolt.DB) error) error {
	db, err := f.getDatabaseForContext(ctx)
	if err != nil {
		return err
	}

	// No FSM lock needed! sync.Map handles concurrency
	return fn(db)
}

func (f *FSM) hookInvalidate(hook physical.InvalidateFunc) {
	f.l.Lock()
	defer f.l.Unlock()

	f.invalidateHook = hook
}

// withDBUpdate executes a read-write function against the database for the given context.
// It handles database retrieval automatically. No FSM locks needed - sync.Map is lock-free!
func (f *FSM) withDBUpdate(ctx context.Context, fn func(*bolt.DB) error) error {
	db, err := f.getDatabaseForContext(ctx)
	if err != nil {
		return err
	}

	// No FSM lock needed! sync.Map handles concurrency
	// BoltDB's Update() provides write serialization per database
	return fn(db)
}

// getDB retrieves or opens a database - NO EXCLUSIVE LOCKS!
func (f *FSM) getDB(filename string) (*bolt.DB, error) {
	// Fast path - lock-free cache lookup
	if entry, ok := f.dbCache.Load(filename); ok {
		cacheEntry := entry.(*dbCacheEntry)

		// Check if expired (only for namespace databases)
		if !cacheEntry.noExpire && time.Now().After(cacheEntry.expiresAt) {
			// Mark as expired, will be reopened
			f.dbCache.Delete(filename)
			// Fall through to slow path
		} else {
			return cacheEntry.db, nil
		}
	}

	// Slow path - use singleflight to ensure only one goroutine opens the database
	// Key insight: singleflight handles all the coordination WITHOUT exclusive locks!
	result, err, shared := f.dbOpenGroup.Do(filename, func() (interface{}, error) {
		// Double-check cache inside singleflight (another goroutine may have opened it)
		if entry, ok := f.dbCache.Load(filename); ok {
			cacheEntry := entry.(*dbCacheEntry)
			if cacheEntry.noExpire || time.Now().Before(cacheEntry.expiresAt) {
				return cacheEntry.db, nil
			}
			// Expired, need to close and reopen
			if err := cacheEntry.db.Close(); err != nil {
				f.logger.Warn("failed to close expired database", "database", filename, "error", err)
			}
			f.dbCache.Delete(filename)
		}

		// Actually open the database file
		db, err := f.openDBFile(filename)
		if err != nil {
			return nil, fmt.Errorf("failed to open database %s: %w", filename, err)
		}

		// Create cache entry with expiration
		entry := &dbCacheEntry{
			db:       db,
			noExpire: filename == databaseFilename(), // Default DB never expires
		}

		if !entry.noExpire {
			entry.expiresAt = time.Now().Add(f.namespaceDBExpiration)
		}

		// Store in cache (lock-free operation!)
		f.dbCache.Store(filename, entry)

		f.logger.Debug("opened database",
			"filename", filename,
			"shared", shared,
			"expires", entry.expiresAt)

		return db, nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*bolt.DB), nil
}

// getAllDatabases returns a list of all database filenames in the FSM directory.
// This includes the default database and all namespace-specific databases.
// The returned list always has the default database first, followed by others in sorted order.
func (f *FSM) getAllDatabases() ([]string, error) {
	return f.findDatabasesInDir(f.path)
}

// findDatabasesInDir returns a list of all database filenames in the specified directory.
// This is used both for the FSM directory and for snapshot directories.
// The returned list always has the default database first, followed by others in sorted order.
func (f *FSM) findDatabasesInDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	var databases []string
	defaultDB := databaseFilename()
	hasDefault := false

	// Find all database files matching the pattern
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		// Check if file matches database pattern: vault*.db
		if strings.HasPrefix(name, databaseFilenameBase) && strings.HasSuffix(name, databaseFilenameExt) {
			if name == defaultDB {
				hasDefault = true
			} else {
				databases = append(databases, name)
			}
		}
	}

	// Sort non-default databases for deterministic ordering
	sort.Strings(databases)

	// Always put default database first if it exists
	if hasDefault {
		databases = append([]string{defaultDB}, databases...)
	}

	// If no databases found, return at least the default
	if len(databases) == 0 {
		databases = []string{defaultDB}
	}

	return databases, nil
}

// runExpirationCleanup runs in background to expire old namespace databases
func (f *FSM) runExpirationCleanup() {
	ticker := time.NewTicker(f.expirationCheckPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f.expireOldDatabases()
		case <-f.stopExpiration:
			return
		}
	}
}

// expireOldDatabases closes and removes expired namespace databases from cache
func (f *FSM) expireOldDatabases() {
	now := time.Now()
	var toExpire []string

	// Collect expired entries
	f.dbCache.Range(func(key, value interface{}) bool {
		filename := key.(string)
		entry := value.(*dbCacheEntry)

		// Skip default database and non-expired entries
		if entry.noExpire || now.Before(entry.expiresAt) {
			return true // continue
		}

		toExpire = append(toExpire, filename)
		return true
	})

	// Close and remove expired databases
	for _, filename := range toExpire {
		if entry, ok := f.dbCache.LoadAndDelete(filename); ok {
			cacheEntry := entry.(*dbCacheEntry)
			if err := cacheEntry.db.Close(); err != nil {
				f.logger.Warn("failed to close expired database",
					"database", filename, "error", err)
			} else {
				f.logger.Debug("expired and closed database", "database", filename)
			}
		}
	}

	if len(toExpire) > 0 {
		f.logger.Info("expired namespace databases", "count", len(toExpire))
	}
}

// SetFSMDelay adds a delay to the FSM apply. This is used in tests to simulate
// a slow apply.
func (r *RaftBackend) SetFSMDelay(delay time.Duration) {
	r.SetFSMApplyCallback(func() { time.Sleep(delay) })
}

func (r *RaftBackend) SetFSMApplyCallback(f func()) {
	r.fsm.l.Lock()
	r.fsm.applyCallback = f
	r.fsm.l.Unlock()
}

func (f *FSM) openDBFile(filename string) (*bolt.DB, error) {
	if len(filename) == 0 {
		return nil, errors.New("can not open empty filename")
	}
	dbPath := filepath.Join(f.path, filename)

	st, err := os.Stat(dbPath)
	switch {
	case err != nil && os.IsNotExist(err):
	case err != nil:
		return nil, fmt.Errorf("error checking raft FSM db file %q: %w", dbPath, err)
	default:
		perms := st.Mode() & os.ModePerm
		if perms&unwantedPermissionBits != 0 {
			f.logger.Warn("raft FSM db file has wider permissions than needed",
				"needed", os.FileMode(dbFilePermissions), "existing", perms)
		}
	}

	opts := boltOptions(dbPath)
	start := time.Now()
	boltDB, err := bolt.Open(dbPath, dbFilePermissions, opts)
	if err != nil {
		return nil, err
	}
	elapsed := time.Since(start)
	f.logger.Debug("time to open database", "elapsed", elapsed, "path", dbPath)
	metrics.MeasureSince([]string{"raft_storage", "fsm", "open_db_file"}, start)

	err = boltDB.Update(func(tx *bolt.Tx) error {
		// make sure we have the necessary buckets created
		_, err := tx.CreateBucketIfNotExists(dataBucketName)
		if err != nil {
			return fmt.Errorf("failed to create bucket: %v", err)
		}
		b, err := tx.CreateBucketIfNotExists(configBucketName)
		if err != nil {
			return fmt.Errorf("failed to create bucket: %v", err)
		}

		// Read in our latest index and term and populate it inmemory
		val := b.Get(latestIndexKey)
		if val != nil {
			var latest IndexValue
			err := proto.Unmarshal(val, &latest)
			if err != nil {
				return err
			}

			atomic.StoreUint64(f.latestTerm, latest.Term)
			atomic.StoreUint64(f.latestIndex, latest.Index)
		}

		// Read in our latest config and populate it inmemory
		val = b.Get(latestConfigKey)
		if val != nil {
			var latest ConfigurationValue
			err := proto.Unmarshal(val, &latest)
			if err != nil {
				return err
			}

			f.latestConfig.Store(&latest)
		}
		return nil
	})

	return boltDB, err
}

func (f *FSM) closeDBFile(filename string) error {
	if len(filename) == 0 {
		return errors.New("cannot close empty filename")
	}

	// Remove from cache and close database
	if entry, ok := f.dbCache.LoadAndDelete(filename); ok {
		cacheEntry := entry.(*dbCacheEntry)
		if err := cacheEntry.db.Close(); err != nil {
			return err
		}
	}

	// Remove database file from disk
	dbPath := filepath.Join(f.path, filename)
	return os.Remove(dbPath)
}

// Stats aggregates and returns bolt database statistics across all open
// database files. This includes metrics for free pages, transactions, and
// storage utilization.
func (f *FSM) Stats() bolt.Stats {
	var stats bolt.Stats

	// Iterate over all cached databases using sync.Map
	f.dbCache.Range(func(key, value interface{}) bool {
		entry := value.(*dbCacheEntry)
		s := entry.db.Stats()

		// Aggregate all stats
		stats.FreePageN += s.FreePageN
		stats.PendingPageN += s.PendingPageN
		stats.FreeAlloc += s.FreeAlloc
		stats.FreelistInuse += s.FreelistInuse
		stats.TxN += s.TxN
		stats.OpenTxN += s.OpenTxN
		stats.TxStats.PageCount += s.TxStats.PageCount
		stats.TxStats.PageAlloc += s.TxStats.PageAlloc
		stats.TxStats.CursorCount += s.TxStats.CursorCount
		stats.TxStats.NodeCount += s.TxStats.NodeCount
		stats.TxStats.NodeDeref += s.TxStats.NodeDeref
		stats.TxStats.Rebalance += s.TxStats.Rebalance
		stats.TxStats.RebalanceTime += s.TxStats.RebalanceTime
		stats.TxStats.Split += s.TxStats.Split
		stats.TxStats.Spill += s.TxStats.Spill
		stats.TxStats.SpillTime += s.TxStats.SpillTime
		stats.TxStats.Write += s.TxStats.Write
		stats.TxStats.WriteTime += s.TxStats.WriteTime

		return true // continue iteration
	})

	return stats
}

func (f *FSM) Close() error {
	// Stop expiration goroutine
	close(f.stopExpiration)

	// Close all databases
	var firstErr error
	f.dbCache.Range(func(key, value interface{}) bool {
		filename := key.(string)
		entry := value.(*dbCacheEntry)

		if err := entry.db.Close(); err != nil {
			f.logger.Error("failed to close database", "database", filename, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}

		return true // continue iteration
	})

	// Clear the cache
	f.dbCache.Range(func(key, value interface{}) bool {
		f.dbCache.Delete(key)
		return true
	})

	return firstErr
}

func writeSnapshotMetaToDB(metadata *raft.SnapshotMeta, db *bolt.DB) error {
	latestIndex := &IndexValue{
		Term:  metadata.Term,
		Index: metadata.Index,
	}
	indexBytes, err := proto.Marshal(latestIndex)
	if err != nil {
		return err
	}

	protoConfig := raftConfigurationToProtoConfiguration(metadata.ConfigurationIndex, metadata.Configuration)
	configBytes, err := proto.Marshal(protoConfig)
	if err != nil {
		return err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(configBucketName)
		if err != nil {
			return err
		}

		err = b.Put(latestConfigKey, configBytes)
		if err != nil {
			return err
		}

		err = b.Put(latestIndexKey, indexBytes)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (f *FSM) localNodeConfig() (*LocalNodeConfigValue, error) {
	var configBytes []byte
	dbDefault, err := f.getDB(databaseFilename())
	if err != nil {
		return nil, err
	}
	if err := dbDefault.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(configBucketName).Get(localNodeConfigKey)
		if value != nil {
			configBytes = make([]byte, len(value))
			copy(configBytes, value)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if configBytes == nil {
		return nil, nil
	}

	var lnConfig LocalNodeConfigValue
	if configBytes != nil {
		err := proto.Unmarshal(configBytes, &lnConfig)
		if err != nil {
			return nil, err
		}
		f.desiredSuffrage = lnConfig.DesiredSuffrage
		return &lnConfig, nil
	}

	return nil, nil
}

func (f *FSM) DesiredSuffrage() string {
	f.l.RLock()
	defer f.l.RUnlock()

	return f.desiredSuffrage
}

func (f *FSM) upgradeLocalNodeConfig() error {
	f.l.Lock()
	defer f.l.Unlock()

	// Read the local node config
	lnConfig, err := f.localNodeConfig()
	if err != nil {
		return err
	}

	// Entry is already present. Get the suffrage value.
	if lnConfig != nil {
		f.desiredSuffrage = lnConfig.DesiredSuffrage
		return nil
	}

	//
	// This is the upgrade case where there is no entry
	//

	lnConfig = &LocalNodeConfigValue{}

	// Refer to the persisted latest raft config
	config := f.latestConfig.Load().(*ConfigurationValue)

	// If there is no config, then this is a fresh node coming up. This could end up
	// being a voter or non-voter. But by default assume that this is a voter. It
	// will be changed if this node joins the cluster as a non-voter.
	if config == nil {
		f.desiredSuffrage = "voter"
		lnConfig.DesiredSuffrage = f.desiredSuffrage
		return f.persistDesiredSuffrage(lnConfig)
	}

	// Get the last known suffrage of the node and assume that it is the desired
	// suffrage. There is no better alternative here.
	for _, srv := range config.Servers {
		if srv.Id == f.localID {
			switch srv.Suffrage {
			case int32(raft.Nonvoter):
				lnConfig.DesiredSuffrage = "non-voter"
			default:
				lnConfig.DesiredSuffrage = "voter"
			}
			// Bring the intent to the fsm instance.
			f.desiredSuffrage = lnConfig.DesiredSuffrage
			break
		}
	}

	return f.persistDesiredSuffrage(lnConfig)
}

// recordSuffrage is called when a node successfully joins the cluster. This
// intent should land in the stored configuration. If the config isn't available
// yet, we still go ahead and store the intent in the fsm. During the next
// update to the configuration, this intent will be persisted.
func (f *FSM) recordSuffrage(desiredSuffrage string) error {
	f.l.Lock()
	defer f.l.Unlock()

	if err := f.persistDesiredSuffrage(&LocalNodeConfigValue{
		DesiredSuffrage: desiredSuffrage,
	}); err != nil {
		return err
	}

	f.desiredSuffrage = desiredSuffrage
	return nil
}

func (f *FSM) persistDesiredSuffrage(lnconfig *LocalNodeConfigValue) error {
	dsBytes, err := proto.Marshal(lnconfig)
	if err != nil {
		return err
	}

	dbDefault, err := f.getDB(databaseFilename())
	if err != nil {
		return err
	}
	return dbDefault.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(configBucketName).Put(localNodeConfigKey, dsBytes)
	})
}

func (f *FSM) witnessSnapshot(metadata *raft.SnapshotMeta) error {
	f.l.RLock()
	defer f.l.RUnlock()

	dbDefault, err := f.getDB(databaseFilename())
	if err == nil {
		err = writeSnapshotMetaToDB(metadata, dbDefault)
	}
	if err != nil {
		return err
	}

	atomic.StoreUint64(f.latestIndex, metadata.Index)
	atomic.StoreUint64(f.latestTerm, metadata.Term)
	f.latestConfig.Store(raftConfigurationToProtoConfiguration(metadata.ConfigurationIndex, metadata.Configuration))

	return nil
}

// LatestState returns the latest index and configuration values we have seen on
// this FSM.
func (f *FSM) LatestState() (*IndexValue, *ConfigurationValue) {
	return &IndexValue{
		Term:  atomic.LoadUint64(f.latestTerm),
		Index: atomic.LoadUint64(f.latestIndex),
	}, f.latestConfig.Load().(*ConfigurationValue)
}

// Delete deletes the given key from the bolt database.
func (f *FSM) Delete(ctx context.Context, path string) error {
	defer metrics.MeasureSince([]string{"raft_storage", "fsm", "delete"}, time.Now())

	return f.withDBUpdate(ctx, func(db *bolt.DB) error {
		return db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket(dataBucketName).Delete([]byte(path))
		})
	})
}

// DeletePrefix removes all keys with the specified prefix from the bolt database.
func (f *FSM) DeletePrefix(ctx context.Context, prefix string) error {
	defer metrics.MeasureSince([]string{"raft_storage", "fsm", "delete_prefix"}, time.Now())

	return f.withDBUpdate(ctx, func(db *bolt.DB) error {
		return db.Update(func(tx *bolt.Tx) error {
			// Assume bucket exists and has keys
			c := tx.Bucket(dataBucketName).Cursor()

			prefixBytes := []byte(prefix)
			for k, _ := c.Seek(prefixBytes); k != nil && bytes.HasPrefix(k, prefixBytes); k, _ = c.Next() {
				if err := c.Delete(); err != nil {
					return err
				}
			}

			return nil
		})
	})
}

// Get retrieves the value at the given path from the bolt database.
func (f *FSM) Get(ctx context.Context, path string) (*physical.Entry, error) {
	// TODO(v2.0): Remove deprecated "raft" metric namespace in a future release.
	// The "raft_storage" namespace should be used instead.
	defer metrics.MeasureSince([]string{"raft", "get"}, time.Now())
	defer metrics.MeasureSince([]string{"raft_storage", "fsm", "get"}, time.Now())

	var valCopy []byte
	var found bool

	err := f.withDBView(ctx, func(db *bolt.DB) error {
		return db.View(func(tx *bolt.Tx) error {
			value := tx.Bucket(dataBucketName).Get([]byte(path))
			if value != nil {
				found = true
				valCopy = make([]byte, len(value))
				copy(valCopy, value)
			}

			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	return &physical.Entry{
		Key:   path,
		Value: valCopy,
	}, nil
}

// Put writes the given entry to the bolt database.
func (f *FSM) Put(ctx context.Context, entry *physical.Entry) error {
	defer metrics.MeasureSince([]string{"raft_storage", "fsm", "put"}, time.Now())

	return f.withDBUpdate(ctx, func(db *bolt.DB) error {
		return db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket(dataBucketName).Put([]byte(entry.Key), entry.Value)
		})
	})
}

// List retrieves the set of keys with the given prefix from the bolt file.
func (f *FSM) List(ctx context.Context, prefix string) ([]string, error) {
	return f.ListPage(ctx, prefix, "", -1)
}

// ListPage retrieves the set of keys with the given prefix from the bolt database,
// after the specified entry (if present), and up to the given limit of entries.
func (f *FSM) ListPage(ctx context.Context, prefix string, after string, limit int) ([]string, error) {
	// TODO(v2.0): Remove deprecated "raft" metric namespace in a future release.
	// The "raft_storage" namespace should be used instead.
	defer metrics.MeasureSince([]string{"raft", "list"}, time.Now())
	defer metrics.MeasureSince([]string{"raft_storage", "fsm", "list"}, time.Now())

	var keys []string
	err := f.withDBView(ctx, func(db *bolt.DB) error {
		return db.View(func(tx *bolt.Tx) error {
			var err error
			keys, err = listPageInner(ctx, tx.Bucket(dataBucketName), prefix, after, limit)
			return err
		})
	})

	return keys, err
}

func listPageInner(ctx context.Context, b *bolt.Bucket, prefix string, after string, limit int) ([]string, error) {
	var keys []string

	prefixBytes := []byte(prefix)
	seekPrefix := []byte(filepath.Join(prefix, after))
	if after == "" {
		seekPrefix = prefixBytes
	} else if !bytes.HasPrefix(seekPrefix, prefixBytes) {
		// filepath.Join has the very unfortunate behavior of trimming the
		// trailing slash when after=".". When e.g., prefix=foo/, this gives
		// us seekPrefix=foo, which fails the initial HasPrefix check,
		// skipping all results.
		seekPrefix = prefixBytes
	}

	// Assume bucket exists and has keys
	c := b.Cursor()

	// By seeking relative to the after location, we can save looking
	// at unnecessary entries before our expected entry.
	for k, _ := c.Seek(seekPrefix); k != nil && bytes.HasPrefix(k, prefixBytes); k, _ = c.Next() {
		if limit > 0 && len(keys) >= limit {
			// We've seen enough entries; exit early.
			return keys, nil
		}

		// Note that we push the comparison of 'key' with 'after'
		// until we add in the directory suffix, if necessary.
		key := string(k)
		key = strings.TrimPrefix(key, prefix)
		if i := strings.Index(key, "/"); i == -1 {
			if after != "" && key <= after {
				// Still prior to our cut-off point, so retry.
				continue
			}

			// Add objects only from the current 'folder'
			keys = append(keys, key)
		} else {
			// Add truncated 'folder' paths
			if len(keys) == 0 || keys[len(keys)-1] != key[:i+1] {
				folder := string(key[:i+1])
				if after != "" && folder <= after {
					// Still prior to our cut-off point, so retry.
					continue
				}

				keys = append(keys, folder)
			}
		}
	}

	return keys, nil
}

// logUnknownOpType logs a warning for an unknown operation type.
// It ensures each unknown type is only logged once using a sync.Map.
func (f *FSM) logUnknownOpType(opType uint32) {
	if _, ok := f.unknownOpTypes.Load(opType); !ok {
		f.logger.Error("unsupported transaction operation", "op", opType)
		f.unknownOpTypes.Store(opType, struct{}{})
	}
}

// applyBatchNonTxOps applies non-transactional operations within ApplyBatch.
// Returns a slice of keys that were successfully written for cache invalidation.
func (f *FSM) applyBatchNonTxOps(b *bolt.Bucket, txnState *fsmTxnCommitIndexApplicationState, command *LogData) ([]string, error) {
	var writtenKeys []string
	for _, op := range command.Operations {
		var err error
		switch op.OpType {
		case putOp:
			err = b.Put([]byte(op.Key), op.Value)
			if err == nil {
				// This log occurs directly to the state tracker, so we
				// want to ensure we only track it when the write succeeded.
				txnState.logWrite(op.Key)
				writtenKeys = append(writtenKeys, op.Key)
			}
		case deleteOp:
			err = b.Delete([]byte(op.Key))
			if err == nil {
				// See note above.
				txnState.logWrite(op.Key)
				writtenKeys = append(writtenKeys, op.Key)
			}
		case restoreCallbackOp:
			if f.restoreCb != nil {
				// Kick off the restore callback function in a go routine
				go f.restoreCb(context.Background())
			}
		default:
			f.logUnknownOpType(op.OpType)
		}

		if err != nil {
			return writtenKeys, err
		}
	}

	return writtenKeys, nil
}

// applyBatchTxOps applies a transaction within the broader context of the batch's transaction.
// It first verifies all operations can apply correctly before performing any writes.
// Returns a slice of keys that were successfully written for cache invalidation.
func (f *FSM) applyBatchTxOps(tx *bolt.Tx, b *bolt.Bucket, txnState *fsmTxnCommitIndexApplicationState, command *LogData) ([]string, error) {
	txnState.setInTx()

	// First we verify the transaction can apply correctly before doing any
	// write operations. This allows us to safely ignore it if it conflicts
	// with previous writes.
	//
	// This assumes that the transaction is constructed so that all verify
	// operations are relative to the initial state of storage and not the
	// in-transaction updated state.
	var err error
	for index, op := range command.Operations {
		switch op.OpType {
		case beginTxOp, commitTxOp:
			// Ensure a well-formed transaction.
			if command.Operations[0].OpType != beginTxOp || command.Operations[len(command.Operations)-1].OpType != commitTxOp || (index != 0 && index != len(command.Operations)-1) {
				return nil, fmt.Errorf("unsupported transaction: saw beginTxOp/commitTxOp mixed inside other operations: %w", physical.ErrTransactionCommitFailure)
			}

			if op.OpType == beginTxOp {
				s, err := parseBeginTxOpValue(op.Value)
				if err != nil {
					return nil, err
				}
				txnState.setStartIndex(s.Index)
			}
		case putOp:
			// ignore
		case deleteOp:
			// ignore
		case verifyReadOp:
			err = txnState.doVerifyRead(b, op)
		case verifyListOp:
			err = txnState.doVerifyList(tx, b, op)
		default:
			f.logUnknownOpType(op.OpType)
		}

		if err != nil {
			return nil, err
		}
	}

	// Now we apply the write operations since the verification succeeded.
	var writtenKeys []string
	for _, op := range command.Operations {
		switch op.OpType {
		case beginTxOp, commitTxOp:
			// ignore
		case putOp:
			err = b.Put([]byte(op.Key), op.Value)
			txnState.logWrite(op.Key)
			writtenKeys = append(writtenKeys, op.Key)
		case deleteOp:
			err = b.Delete([]byte(op.Key))
			txnState.logWrite(op.Key)
			writtenKeys = append(writtenKeys, op.Key)
		case verifyReadOp:
			// ignore
		case verifyListOp:
			// ignore
		default:
			f.logUnknownOpType(op.OpType)
		}

		if err != nil {
			return writtenKeys, err
		}
	}

	// Record the transaction as having been applied and merge state back into
	// the central fast application tracking.
	txnState.finishTxn()

	return writtenKeys, nil
}

/*
// ApplyBatchOld will apply a set of logs to the FSM. This is called from the raft
// library.
func (f *FSM) ApplyBatchOld(logs []*raft.Log) []interface{} {
	numLogs := len(logs)

	if numLogs == 0 {
		return []interface{}{}
	}

	// We will construct one slice per log, each slice containing another slice of results from our get ops
	entrySlices := make([][]*FSMEntry, 0, numLogs)

	// Do the unmarshalling first so we don't hold locks
	var latestConfiguration *ConfigurationValue
	commands := make([]interface{}, 0, numLogs)
	for _, l := range logs {
		switch l.Type {
		case raft.LogCommand:
			command := &LogData{}
			err := proto.Unmarshal(l.Data, command)
			if err != nil {
				f.logger.Error("error proto unmarshaling log data", "error", err)
				panic("error proto unmarshaling log data")
			}
			commands = append(commands, command)
		case raft.LogConfiguration:
			configuration := raft.DecodeConfiguration(l.Data)
			config := raftConfigurationToProtoConfiguration(l.Index, configuration)

			commands = append(commands, config)

			// Update the latest configuration the fsm has received; we will
			// store this after it has been committed to storage.
			latestConfiguration = config

		default:
			panic(fmt.Sprintf("got unexpected log type: %d", l.Type))
		}
	}

	// Only advance latest pointer if this log has a higher index value than
	// what we have seen in the past.
	var logIndex []byte
	var err error
	latestIndex, _ := f.LatestState()
	lastLog := logs[numLogs-1]
	if latestIndex.Index < lastLog.Index {
		logIndex, err = proto.Marshal(&IndexValue{
			Term:  lastLog.Term,
			Index: lastLog.Index,
		})
		if err != nil {
			f.logger.Error("unable to marshal latest index", "error", err)
			panic("unable to marshal latest index")
		}
	}

	f.l.RLock()
	defer f.l.RUnlock()

	if f.applyCallback != nil {
		f.applyCallback()
	}

	var lowestActiveIndex *uint64

	// One would think that this f.db.Update(...) and the following loop over
	// commands should be in the opposite order, as we want transactions to be
	// applied atomically. Indeed, 2c154ad516162dcb8b15ad270cd6a15516f2ce59 had
	// this ordered that way. It has two issues though:
	//
	// 1. It is slower, as each bbolt transaction incurs additional storage
	//    writes.
	// 2. Technically, Raft expects the entire batch to succeed or fail as a
	//    unit; thus, we don't want to commit partial state from a previous
	//    log entry (that succeeded) when a later log entry fails.
	//
	// Hence, keep the original upstream ordering of Update w.r.t. batch
	// application and switch to pre-verifying transactions prior tok,
	// performing any writes in them.
	dbDefault, err := f.getDB(databaseFilename())
	if err != nil {
		f.logger.Error("failed to get default database", "error", err)
		panic("failed to get default database")
	}
	err = dbDefault.Update(func(tx *bolt.Tx) error {
		configB := tx.Bucket(configBucketName)
		latestIndex := atomic.LoadUint64(f.latestIndex)

		for commandIndex, commandRaw := range commands {
			entrySlice := make([]*FSMEntry, 0, 1)

			switch command := commandRaw.(type) {
			case *LogData:
				txnState := f.fastTxnTracker.applyState(latestIndex, commandIndex, logs[commandIndex].Index)
				b := tx.Bucket(dataBucketName)
				if len(command.Operations) == 0 || command.Operations[0].OpType != beginTxOp {
					_, err = f.applyBatchNonTxOps(b, txnState, command)
				} else {
					_, err = f.applyBatchTxOps(tx, b, txnState, command)
				}

				if command.LowestActiveIndex != nil {
					lowestActiveIndex = command.LowestActiveIndex
				}

				if err != nil {
					// If we're in a transaction, we do not want to err the
					// global f.db.Update call unless this is a critical error
					// worthy of a panic(...).
					//
					// Create a special FSMEntry to send back the error
					// message, that applyLog(...) will look for if it
					// sent a transaction.
					if txnState.getInTx() && errors.Is(err, physical.ErrTransactionCommitFailure) {
						entrySlice = append(entrySlice, &FSMEntry{
							Key:   fsmEntryTxErrorKey,
							Value: []byte(err.Error()),
						})

						// Process other events; this transaction failure was handled
						// appropriately already in applyBatchTxOps.
						err = nil
					}
				}
			case *ConfigurationValue:
				configBytes, err := proto.Marshal(command)
				if err != nil {
					return err
				}
				if err := configB.Put(latestConfigKey, configBytes); err != nil {
					return err
				}
			}

			entrySlices = append(entrySlices, entrySlice)

			if err != nil {
				break
			}
		}

		return err
	})

	// If we had no error, update our last applied log.
	if err == nil {
		err = dbDefault.Update(func(tx *bolt.Tx) error {
			if len(logIndex) > 0 {
				b := tx.Bucket(configBucketName)
				err = b.Put(latestIndexKey, logIndex)
				if err != nil {
					return err
				}
			}

			return nil
		})
	}

	if err != nil {
		f.logger.Error("failed to store data", "error", err)
		panic("failed to store data")
	}

	if lowestActiveIndex != nil {
		f.fastTxnTracker.clearOldEntries(*lowestActiveIndex)
	}

	// If we advanced the latest value, update the in-memory representation too.
	if len(logIndex) > 0 {
		atomic.StoreUint64(f.latestTerm, lastLog.Term)
		atomic.StoreUint64(f.latestIndex, lastLog.Index)
	}

	// If one or more configuration changes were processed, store the latest one.
	if latestConfiguration != nil {
		f.latestConfig.Store(latestConfiguration)
	}

	// Build the responses. The logs array is used here to ensure we reply to
	// all command values; even if they are not of the types we expect. This
	// should futureproof this function from more log types being provided.
	resp := make([]interface{}, numLogs)
	for i := range logs {
		resp[i] = &FSMApplyResponse{
			Success:    true,
			EntrySlice: entrySlices[i],
		}
	}

	return resp
}

// this is copied from the latest branch 0dcb577 before "Improve raft transaction application performance (#634)"
// ApplyBatchTry will apply a set of logs to the FSM. This is called from the raft
// library.
func (f *FSM) ApplyBatchTry(logs []*raft.Log) []interface{} {
	numLogs := len(logs)

	if numLogs == 0 {
		return []interface{}{}
	}

	dbList := make([]*bolt.DB, numLogs)
	dbName := make([]string, numLogs)

	// We will construct one slice per log, each slice containing another slice of results from our get ops
	entrySlices := make([][]*FSMEntry, 0, numLogs)

	// Do the unmarshalling first so we don't hold locks
	var latestConfiguration *ConfigurationValue
	commands := make([]interface{}, 0, numLogs)
	for i, l := range logs {
		vaultDBName := databaseFilename()
		switch l.Type {
		case raft.LogCommand:
			command := &LogData{}
			err := proto.Unmarshal(l.Data, command)
			if err != nil {
				f.logger.Error("error proto unmarshaling log data", "error", err)
				panic("error proto unmarshaling log data")
			}
			commands[i] = command
			if command.BucketName != nil {
				vaultDBName = *command.BucketName
			}
		case raft.LogConfiguration:
			configuration := raft.DecodeConfiguration(l.Data)
			config := raftConfigurationToProtoConfiguration(l.Index, configuration)

			commands[i] = config

			// Update the latest configuration the fsm has received; we will
			// store this after it has been committed to storage.
			latestConfiguration = config
		default:
			panic(fmt.Sprintf("got unexpected log type: %d", l.Type))
		}
		db, err := f.getDB(vaultDBName)
		if err != nil {
			f.logger.Error("unable to get db", "error", err, "bucket", vaultDBName)
			panic("unable to get db")
		}
		dbName[i] = vaultDBName
		dbList[i] = db
	}
	// Only advance latest pointer if this log has a higher index value than
	// what we have seen in the past.
	var logIndex []byte
	var err error
	latestIndex, _ := f.LatestState()
	lastLog := logs[numLogs-1]
	if latestIndex.Index < lastLog.Index {
		logIndex, err = proto.Marshal(&IndexValue{
			Term:  lastLog.Term,
			Index: lastLog.Index,
		})
		if err != nil {
			f.logger.Error("unable to marshal latest index", "error", err)
			panic("unable to marshal latest index")
		}
	}

	latestIndexAtomic := atomic.LoadUint64(f.latestIndex)

	var lowestActiveIndex *uint64

	f.l.RLock()
	defer f.l.RUnlock()

	if f.applyCallback != nil {
		f.applyCallback()
	}

	// This loop and subsequent f.db.Update call were reordered from upstream,
	// to enable one transaction per command. Raft chunking will already be
	// applied, so if logs are split across multiple operations, we'll see
	// only a single call here. This ensures that our transaction really will
	// apply at the bolt level, independent of the other operations occurring,
	// and lets us back out just the changes in the transaction if they fail
	// to apply.
	for commandIndex, commandRaw := range commands {
		inTx := false
		entrySlice := make([]*FSMEntry, 0)
		err = dbList[commandIndex].Update(func(tx *bolt.Tx) error {
			switch command := commandRaw.(type) {
			case *LogData:
				b := tx.Bucket(dataBucketName)
				txnState := f.fastTxnTracker.applyState(latestIndexAtomic, commandIndex, logs[commandIndex].Index)
				if len(command.Operations) == 0 || command.Operations[0].OpType != beginTxOp {
					_, err = f.applyBatchNonTxOps(b, txnState, command)
				} else {
					_, err = f.applyBatchTxOps(tx, b, txnState, command)
				}

				if command.LowestActiveIndex != nil {
					lowestActiveIndex = command.LowestActiveIndex
				}

				if err != nil {
					// If we're in a transaction, we do not want to err the
					// global f.db.Update call unless this is a critical error
					// worthy of a panic(...).
					//
					// Create a special FSMEntry to send back the error
					// message, that applyLog(...) will look for if it
					// sent a transaction.
					if txnState.getInTx() && errors.Is(err, physical.ErrTransactionCommitFailure) {
						entrySlice = append(entrySlice, &FSMEntry{
							Key:   fsmEntryTxErrorKey,
							Value: []byte(err.Error()),
						})

						// Process other events; this transaction failure was handled
						// appropriately already in applyBatchTxOps.
						err = nil
					}
				}
			case *ConfigurationValue:
				b := tx.Bucket(configBucketName)
				configBytes, err := proto.Marshal(command)
				if err != nil {
					return err
				}
				if err := b.Put(latestConfigKey, configBytes); err != nil {
					return err
				}
			}

			return nil
		})

		entrySlices[commandIndex] = entrySlice

		if err != nil && inTx && errors.Is(err, physical.ErrTransactionCommitFailure) {
			// Process other events; this transaction failure was handled
			// appropriately.
			err = nil
			continue
		}

		if err != nil {
			// Unknown error type or non-transactional error; exit and panic.
			break
		}
	}

	if err == nil {
		var db *bolt.DB
		db, err = f.getDB(databaseFilename())
		if err == nil {
			err = db.Update(func(tx *bolt.Tx) error {
				if len(logIndex) > 0 {
					b := tx.Bucket(configBucketName)
					err = b.Put(latestIndexKey, logIndex)
					if err != nil {
						return err
					}
				}

				return nil
			})
		}
	}

	if err != nil {
		f.logger.Error("failed to store data", "error", err)
		panic("failed to store data")
	}

	if lowestActiveIndex != nil {
		f.fastTxnTracker.clearOldEntries(*lowestActiveIndex)
	}

	// If we advanced the latest value, update the in-memory representation too.
	if len(logIndex) > 0 {
		atomic.StoreUint64(f.latestTerm, lastLog.Term)
		atomic.StoreUint64(f.latestIndex, lastLog.Index)
	}

	// If one or more configuration changes were processed, store the latest one.
	if latestConfiguration != nil {
		f.latestConfig.Store(latestConfiguration)
	}

	// Build the responses. The logs array is used here to ensure we reply to
	// all command values; even if they are not of the types we expect. This
	// should futureproof this function from more log types being provided.
	resp := make([]interface{}, numLogs)
	for i := range logs {
		resp[i] = &FSMApplyResponse{
			Success:    true,
			EntrySlice: entrySlices[i],
		}
	}

	return resp
}
*/

// ApplyBatch combines multi-database support from ApplyBatchTry with the single
// transaction pattern from ApplyBatchOld. It groups commands by database and
// executes one transaction per unique database, rather than one transaction per
// command or one transaction for all commands.
func (f *FSM) ApplyBatch(logs []*raft.Log) []interface{} {
	numLogs := len(logs)

	if numLogs == 0 {
		return []interface{}{}
	}

	dbList := make([]*bolt.DB, numLogs)
	dbName := make([]string, numLogs)

	// We will construct one slice per log, each slice containing another slice of results from our get ops
	entrySlices := make([][]*FSMEntry, 0, numLogs)

	// Do the unmarshalling first so we don't hold locks
	var latestConfiguration *ConfigurationValue
	commands := make([]interface{}, 0, numLogs)
	for i, l := range logs {
		vaultDBName := databaseFilename()
		switch l.Type {
		case raft.LogCommand:
			command := &LogData{}
			err := proto.Unmarshal(l.Data, command)
			if err != nil {
				f.logger.Error("error proto unmarshaling log data", "error", err)
				panic("error proto unmarshaling log data")
			}
			commands = append(commands, command)
			if command.BucketName != nil {
				vaultDBName = *command.BucketName
			}
		case raft.LogConfiguration:
			configuration := raft.DecodeConfiguration(l.Data)
			config := raftConfigurationToProtoConfiguration(l.Index, configuration)

			commands = append(commands, config)

			// Update the latest configuration the fsm has received; we will
			// store this after it has been committed to storage.
			latestConfiguration = config
		default:
			panic(fmt.Sprintf("got unexpected log type: %d", l.Type))
		}
		db, err := f.getDB(vaultDBName)
		if err != nil {
			f.logger.Error("unable to get db", "error", err, "bucket", vaultDBName)
			panic("unable to get db")
		}
		dbName[i] = vaultDBName
		dbList[i] = db
	}

	// Only advance latest pointer if this log has a higher index value than
	// what we have seen in the past.
	var logIndex []byte
	var err error
	latestIndex, _ := f.LatestState()
	lastLog := logs[numLogs-1]
	if latestIndex.Index < lastLog.Index {
		logIndex, err = proto.Marshal(&IndexValue{
			Term:  lastLog.Term,
			Index: lastLog.Index,
		})
		if err != nil {
			f.logger.Error("unable to marshal latest index", "error", err)
			panic("unable to marshal latest index")
		}
	}

	latestIndexAtomic := atomic.LoadUint64(f.latestIndex)

	var lowestActiveIndex *uint64

	f.l.RLock()
	defer f.l.RUnlock()

	if f.applyCallback != nil {
		f.applyCallback()
	}

	// Track keys that were successfully written for cache invalidation
	var writtenKeys []string

	// Group commands by database for efficient batch processing.
	// Use a single transaction per unique database (like ApplyBatchOld),
	// but support multiple databases (like ApplyBatch).
	type dbCommandGroup struct {
		db           *bolt.DB
		dbName       string
		commandInfos []struct {
			index      int
			commandRaw interface{}
			log        *raft.Log
		}
	}

	// Build groups of commands per database
	dbGroups := make(map[string]*dbCommandGroup)
	for commandIndex, commandRaw := range commands {
		dbNameKey := dbName[commandIndex]
		if _, exists := dbGroups[dbNameKey]; !exists {
			dbGroups[dbNameKey] = &dbCommandGroup{
				db:     dbList[commandIndex],
				dbName: dbNameKey,
				commandInfos: make([]struct {
					index      int
					commandRaw interface{}
					log        *raft.Log
				}, 0),
			}
		}
		dbGroups[dbNameKey].commandInfos = append(dbGroups[dbNameKey].commandInfos, struct {
			index      int
			commandRaw interface{}
			log        *raft.Log
		}{
			index:      commandIndex,
			commandRaw: commandRaw,
			log:        logs[commandIndex],
		})
	}

	// Initialize entry slices for all commands
	for i := 0; i < numLogs; i++ {
		entrySlices = append(entrySlices, make([]*FSMEntry, 0))
	}

	// Process each database group with a single transaction
	for _, group := range dbGroups {
		err = group.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(dataBucketName)
			configB := tx.Bucket(configBucketName)

			for _, cmdInfo := range group.commandInfos {
				switch command := cmdInfo.commandRaw.(type) {
				case *LogData:
					txnState := f.fastTxnTracker.applyState(latestIndexAtomic, cmdInfo.index, cmdInfo.log.Index)
					var opWrittenKeys []string
					if len(command.Operations) == 0 || command.Operations[0].OpType != beginTxOp {
						opWrittenKeys, err = f.applyBatchNonTxOps(b, txnState, command)
					} else {
						opWrittenKeys, err = f.applyBatchTxOps(tx, b, txnState, command)
					}

					// Only add to writtenKeys if the operation succeeded
					if err == nil {
						writtenKeys = append(writtenKeys, opWrittenKeys...)
					}

					if command.LowestActiveIndex != nil {
						lowestActiveIndex = command.LowestActiveIndex
					}

					if err != nil {
						// If we're in a transaction, we do not want to err the
						// global db.Update call unless this is a critical error
						// worthy of a panic(...).
						//
						// Create a special FSMEntry to send back the error
						// message, that applyLog(...) will look for if it
						// sent a transaction.
						if txnState.getInTx() && errors.Is(err, physical.ErrTransactionCommitFailure) {
							entrySlices[cmdInfo.index] = append(entrySlices[cmdInfo.index], &FSMEntry{
								Key:   fsmEntryTxErrorKey,
								Value: []byte(err.Error()),
							})

							// Process other events; this transaction failure was handled
							// appropriately already in applyBatchTxOps.
							err = nil
						}
					}
				case *ConfigurationValue:
					configBytes, err := proto.Marshal(command)
					if err != nil {
						return err
					}
					if err := configB.Put(latestConfigKey, configBytes); err != nil {
						return err
					}
				}

				if err != nil {
					return err
				}
			}

			return nil
		})
		if err != nil {
			break
		}
	}

	// If we had no error, update our last applied log in the default database.
	if err == nil {
		dbDefault, getErr := f.getDB(databaseFilename())
		if getErr != nil {
			f.logger.Error("failed to get default database", "error", getErr)
			panic("failed to get default database")
		}
		err = dbDefault.Update(func(tx *bolt.Tx) error {
			if len(logIndex) > 0 {
				b := tx.Bucket(configBucketName)
				err = b.Put(latestIndexKey, logIndex)
				if err != nil {
					return err
				}
			}

			return nil
		})
	}

	if err != nil {
		f.logger.Error("failed to store data", "error", err)
		panic("failed to store data")
	}

	if lowestActiveIndex != nil {
		f.fastTxnTracker.clearOldEntries(*lowestActiveIndex)
	}

	// Capture hook locally to avoid race with concurrent hookInvalidate() calls.
	// Only invalidate keys that were actually written successfully.
	hook := f.invalidateHook
	if hook != nil && len(writtenKeys) > 0 {
		go hook(writtenKeys...)
	}

	// If we advanced the latest value, update the in-memory representation too.
	if len(logIndex) > 0 {
		atomic.StoreUint64(f.latestTerm, lastLog.Term)
		atomic.StoreUint64(f.latestIndex, lastLog.Index)
	}

	// If one or more configuration changes were processed, store the latest one.
	if latestConfiguration != nil {
		f.latestConfig.Store(latestConfiguration)
	}

	// Build the responses. The logs array is used here to ensure we reply to
	// all command values; even if they are not of the types we expect. This
	// should futureproof this function from more log types being provided.
	resp := make([]interface{}, numLogs)
	for i := range logs {
		resp[i] = &FSMApplyResponse{
			Success:    true,
			EntrySlice: entrySlices[i],
		}
	}

	return resp
}

// Apply will apply a log value to the FSM. This is called from the raft
// library.
func (f *FSM) Apply(log *raft.Log) interface{} {
	return f.ApplyBatch([]*raft.Log{log})[0]
}

type writeErrorCloser interface {
	io.WriteCloser
	CloseWithError(error) error
}

// writeTo will copy the FSM's content to a remote sink. The data is written
// twice, once for use in determining various metadata attributes of the dataset
// (size, checksum, etc) and a second for the sink of the data. We also use a
// proto delimited writer so we can stream proto messages to the sink.
//
// For multi-database mode, this function iterates through all databases and
// writes them to the snapshot. Database boundaries are marked with special
// marker entries using the __DATABASE__: prefix.
func (f *FSM) writeTo(ctx context.Context, metaSink writeErrorCloser, sink writeErrorCloser) {
	defer metrics.MeasureSince([]string{"raft_storage", "fsm", "write_snapshot"}, time.Now())

	protoWriter := NewDelimitedWriter(sink)
	metadataProtoWriter := NewDelimitedWriter(metaSink)

	// Get all databases to snapshot
	dbNames, err := f.getAllDatabases()
	if err != nil {
		sink.CloseWithError(err)
		metaSink.CloseWithError(err)
		return
	}

	f.l.RLock()
	defer f.l.RUnlock()

	// Iterate through all databases
	for _, dbName := range dbNames {
		db, err := f.getDB(dbName)
		if err != nil {
			sink.CloseWithError(err)
			metaSink.CloseWithError(err)
			return
		}

		// Write database marker to both sinks
		markerKey := "__DATABASE__:" + dbName
		markerEntry := &pb.StorageEntry{
			Key:   markerKey,
			Value: []byte(dbName),
		}

		if err := metadataProtoWriter.WriteMsg(markerEntry); err != nil {
			metaSink.CloseWithError(err)
			return
		}

		if err := protoWriter.WriteMsg(markerEntry); err != nil {
			sink.CloseWithError(err)
			return
		}

		// Write all entries from this database
		err = db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(dataBucketName)
			if b == nil {
				// Empty database, skip
				return nil
			}
			c := b.Cursor()

			// Do the first scan of the data for metadata purposes.
			for k, v := c.First(); k != nil; k, v = c.Next() {
				err := metadataProtoWriter.WriteMsg(&pb.StorageEntry{
					Key:   string(k),
					Value: v,
				})
				if err != nil {
					return err
				}
			}

			// Do the second scan for copy purposes.
			for k, v := c.First(); k != nil; k, v = c.Next() {
				err := protoWriter.WriteMsg(&pb.StorageEntry{
					Key:   string(k),
					Value: v,
				})
				if err != nil {
					return err
				}
			}

			return nil
		})
		if err != nil {
			sink.CloseWithError(err)
			metaSink.CloseWithError(err)
			return
		}
	}

	metaSink.Close()
	sink.CloseWithError(nil)
}

// Snapshot implements the FSM interface. It returns a noop snapshot object.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &noopSnapshotter{
		fsm: f,
	}, nil
}

// SetNoopRestore is used to disable restore operations on raft startup. Because
// we are using persistent storage in our FSM we do not need to issue a restore
// on startup.
func (f *FSM) SetNoopRestore(enabled bool) {
	f.l.Lock()
	f.noopRestore = enabled
	f.l.Unlock()
}

// Restore installs a new snapshot from the provided reader. It does an atomic
// rename of the snapshot file(s) into the database filepath(s). While a restore is
// happening the FSM is locked and no writes or reads can be performed.
//
// For multi-database mode, this function installs all database files from the
// snapshot directory.
func (f *FSM) Restore(r io.ReadCloser) error {
	defer metrics.MeasureSince([]string{"raft_storage", "fsm", "restore_snapshot"}, time.Now())

	if f.noopRestore {
		return nil
	}

	snapshotInstaller, ok := r.(*boltSnapshotInstaller)
	if !ok {
		wrapper, ok := r.(raft.ReadCloserWrapper)
		if !ok {
			return fmt.Errorf("expected ReadCloserWrapper object, got: %T", r)
		}
		snapshotInstallerRaw := wrapper.WrappedReadCloser()
		snapshotInstaller, ok = snapshotInstallerRaw.(*boltSnapshotInstaller)
		if !ok {
			return fmt.Errorf("expected snapshot installer object, got: %T", snapshotInstallerRaw)
		}
	}

	f.l.Lock()
	defer f.l.Unlock()

	lnConfig, err := f.localNodeConfig()
	if err != nil {
		return err
	}

	// Close ALL currently open databases
	f.logger.Info("closing all open databases for snapshot restore")
	var closeErrors []error
	currentDBs, err := f.getAllDatabases()
	if err != nil {
		f.logger.Warn("failed to get all databases, closing default only", "error", err)
		currentDBs = []string{databaseFilename()}
	}

	for _, dbName := range currentDBs {
		db, ok := f.dbCache.Get(dbName)
		if ok && db != nil {
			if closeErr := db.Close(); closeErr != nil {
				f.logger.Error("failed to close database", "database", dbName, "error", closeErr)
				closeErrors = append(closeErrors, closeErr)
			} else {
				f.logger.Debug("closed database", "database", dbName)
			}
		}
	}

	// Clear the database cache
	f.dbCache.DeleteAll()

	f.logger.Info("installing snapshot to FSM")

	// Find all database files in the snapshot directory
	snapshotDir := filepath.Dir(snapshotInstaller.filename)
	snapshotDBs, err := f.findDatabasesInDir(snapshotDir)
	if err != nil {
		f.logger.Error("failed to find databases in snapshot", "error", err)
		return fmt.Errorf("failed to find databases in snapshot: %w", err)
	}

	if len(snapshotDBs) == 0 {
		f.logger.Warn("no database files found in snapshot, falling back to single database restore")
		snapshotDBs = []string{databaseFilename()}
	}

	f.logger.Info("found databases in snapshot", "count", len(snapshotDBs), "databases", snapshotDBs)

	// Install all database files
	var retErr *multierror.Error
	for _, dbName := range snapshotDBs {
		srcPath := filepath.Join(snapshotDir, dbName)
		dstPath := filepath.Join(f.path, dbName)

		f.logger.Info("installing database", "database", dbName, "from", srcPath, "to", dstPath)

		var installErr error
		if runtime.GOOS != "windows" {
			installErr = safeio.Rename(srcPath, dstPath)
		} else {
			installErr = os.Rename(srcPath, dstPath)
		}

		if installErr != nil {
			f.logger.Error("failed to install database", "database", dbName, "error", installErr)
			retErr = multierror.Append(retErr, fmt.Errorf("failed to install database %s: %w", dbName, installErr))
		} else {
			f.logger.Info("installed database", "database", dbName)
		}
	}

	// Reopen all installed databases
	f.logger.Info("reopening all databases")
	for _, dbName := range snapshotDBs {
		if _, err := f.openDBFile(dbName); err != nil {
			f.logger.Error("failed to open database file", "database", dbName, "error", err)
			retErr = multierror.Append(retErr, fmt.Errorf("failed to open database %s: %w", dbName, err))
		} else {
			f.logger.Info("reopened database", "database", dbName)
		}
	}

	// Handle local node config restore. lnConfig should not be nil here, but
	// adding the nil check anyways for safety.
	if lnConfig != nil {
		// Persist the local node config on the restored fsm.
		if err := f.persistDesiredSuffrage(lnConfig); err != nil {
			f.logger.Error("failed to persist local node config from before the restore", "error", err)
			retErr = multierror.Append(retErr, fmt.Errorf("failed to persist local node config from before the restore: %w", err))
		}
	}

	return retErr.ErrorOrNil()
}

// noopSnapshotter implements the fsm.Snapshot interface. It doesn't do anything
// since our SnapshotStore reads data out of the FSM on Open().
type noopSnapshotter struct {
	fsm *FSM
}

// Persist implements the fsm.Snapshot interface. It doesn't need to persist any
// state data, but it does persist the raft metadata. This is necessary so we
// can be sure to capture indexes for operation types that are not sent to the
// FSM.
func (s *noopSnapshotter) Persist(sink raft.SnapshotSink) error {
	boltSnapshotSink := sink.(*BoltSnapshotSink)

	// We are processing a snapshot, fastforward the index, term, and
	// configuration to the latest seen by the raft system.
	if err := s.fsm.witnessSnapshot(&boltSnapshotSink.meta); err != nil {
		return err
	}

	return nil
}

// Release doesn't do anything.
func (s *noopSnapshotter) Release() {}

// raftConfigurationToProtoConfiguration converts a raft configuration object to
// a proto value.
func raftConfigurationToProtoConfiguration(index uint64, configuration raft.Configuration) *ConfigurationValue {
	servers := make([]*Server, len(configuration.Servers))
	for i, s := range configuration.Servers {
		servers[i] = &Server{
			Suffrage: int32(s.Suffrage),
			Id:       string(s.ID),
			Address:  string(s.Address),
		}
	}
	return &ConfigurationValue{
		Index:   index,
		Servers: servers,
	}
}

// protoConfigurationToRaftConfiguration converts a proto configuration object
// to a raft object.
func protoConfigurationToRaftConfiguration(configuration *ConfigurationValue) (uint64, raft.Configuration) {
	servers := make([]raft.Server, len(configuration.Servers))
	for i, s := range configuration.Servers {
		servers[i] = raft.Server{
			Suffrage: raft.ServerSuffrage(s.Suffrage),
			ID:       raft.ServerID(s.Id),
			Address:  raft.ServerAddress(s.Address),
		}
	}
	return configuration.Index, raft.Configuration{
		Servers: servers,
	}
}

type FSMChunkStorage struct {
	f   *FSM
	ctx context.Context
}

// chunkPaths returns a disk prefix and key given chunkinfo
func (f *FSMChunkStorage) chunkPaths(chunk *raftchunking.ChunkInfo) (string, string) {
	prefix := fmt.Sprintf("%s%d/", chunkingPrefix, chunk.OpNum)
	key := fmt.Sprintf("%s%d", prefix, chunk.SequenceNum)
	return prefix, key
}

func (f *FSMChunkStorage) StoreChunk(chunk *raftchunking.ChunkInfo) (bool, error) {
	b, err := jsonutil.EncodeJSON(chunk)
	if err != nil {
		return false, fmt.Errorf("error encoding chunk info: %w", err)
	}

	prefix, key := f.chunkPaths(chunk)

	entry := &physical.Entry{
		Key:   key,
		Value: b,
	}

	db, err := f.f.getDatabaseForContext(f.ctx)
	if err != nil {
		return false, err
	}

	f.f.l.RLock()
	defer f.f.l.RUnlock()

	// Start a write transaction.
	done := new(bool)
	if err := db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(dataBucketName).Put([]byte(entry.Key), entry.Value); err != nil {
			return fmt.Errorf("error storing chunk info: %w", err)
		}

		// Assume bucket exists and has keys
		c := tx.Bucket(dataBucketName).Cursor()

		var keys []string
		prefixBytes := []byte(prefix)
		for k, _ := c.Seek(prefixBytes); k != nil && bytes.HasPrefix(k, prefixBytes); k, _ = c.Next() {
			key := string(k)
			key = strings.TrimPrefix(key, prefix)
			if i := strings.Index(key, "/"); i == -1 {
				// Add objects only from the current 'folder'
				keys = append(keys, key)
			} else {
				// Add truncated 'folder' paths
				keys = strutil.AppendIfMissing(keys, string(key[:i+1]))
			}
		}

		*done = uint32(len(keys)) == chunk.NumChunks

		return nil
	}); err != nil {
		return false, err
	}

	return *done, nil
}

func (f *FSMChunkStorage) FinalizeOp(opNum uint64) ([]*raftchunking.ChunkInfo, error) {
	ret, err := f.chunksForOpNum(opNum)
	if err != nil {
		return nil, fmt.Errorf("error getting chunks for op keys: %w", err)
	}

	prefix, _ := f.chunkPaths(&raftchunking.ChunkInfo{OpNum: opNum})
	if err := f.f.DeletePrefix(f.ctx, prefix); err != nil {
		return nil, fmt.Errorf("error deleting prefix after op finalization: %w", err)
	}

	return ret, nil
}

func (f *FSMChunkStorage) chunksForOpNum(opNum uint64) ([]*raftchunking.ChunkInfo, error) {
	prefix, _ := f.chunkPaths(&raftchunking.ChunkInfo{OpNum: opNum})

	opChunkKeys, err := f.f.List(f.ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("error fetching op chunk keys: %w", err)
	}

	if len(opChunkKeys) == 0 {
		return nil, nil
	}

	var ret []*raftchunking.ChunkInfo

	for _, v := range opChunkKeys {
		seqNum, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error converting seqnum to integer: %w", err)
		}

		entry, err := f.f.Get(f.ctx, prefix+v)
		if err != nil {
			return nil, fmt.Errorf("error fetching chunkinfo: %w", err)
		}

		var ci raftchunking.ChunkInfo
		if err := jsonutil.DecodeJSON(entry.Value, &ci); err != nil {
			return nil, fmt.Errorf("error decoding chunkinfo json: %w", err)
		}

		if ret == nil {
			ret = make([]*raftchunking.ChunkInfo, ci.NumChunks)
		}

		ret[seqNum] = &ci
	}

	return ret, nil
}

func (f *FSMChunkStorage) GetChunks() (raftchunking.ChunkMap, error) {
	opNums, err := f.f.List(f.ctx, chunkingPrefix)
	if err != nil {
		return nil, fmt.Errorf("error doing recursive list for chunk saving: %w", err)
	}

	if len(opNums) == 0 {
		return nil, nil
	}

	ret := make(raftchunking.ChunkMap, len(opNums))
	for _, opNumStr := range opNums {
		opNum, err := strconv.ParseInt(opNumStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing op num during chunk saving: %w", err)
		}

		opChunks, err := f.chunksForOpNum(uint64(opNum))
		if err != nil {
			return nil, fmt.Errorf("error getting chunks for op keys during chunk saving: %w", err)
		}

		ret[uint64(opNum)] = opChunks
	}

	return ret, nil
}

func (f *FSMChunkStorage) RestoreChunks(chunks raftchunking.ChunkMap) error {
	if err := f.f.DeletePrefix(f.ctx, chunkingPrefix); err != nil {
		return fmt.Errorf("error deleting prefix for chunk restoration: %w", err)
	}
	if len(chunks) == 0 {
		return nil
	}

	for opNum, opChunks := range chunks {
		for _, chunk := range opChunks {
			if chunk == nil {
				continue
			}
			if chunk.OpNum != opNum {
				return errors.New("unexpected op number in chunk")
			}
			if _, err := f.StoreChunk(chunk); err != nil {
				return fmt.Errorf("error storing chunk during restoration: %w", err)
			}
		}
	}

	return nil
}
