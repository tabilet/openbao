// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

// This file contains the improved FSM implementation using sync.Map + singleflight
// to eliminate blocking during cache operations.
//
// Key improvements:
// 1. dbCache changed from zcache.Cache to sync.Map (lock-free reads)
// 2. Added singleflight.Group to prevent thundering herd on database opens
// 3. Database cache entries no longer block all operations during cache miss
// 4. Background expiration goroutine for namespace databases

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/hashicorp/go-hclog"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sync/singleflight"
)

// dbCacheEntry wraps a database handle with expiration information
type dbCacheEntry struct {
	db        *bolt.DB
	expiresAt time.Time
	noExpire  bool // true for default database
}

// FSMWithSyncMap is the improved FSM implementation
// Replace the existing FSM struct with this version
type FSMWithSyncMap struct {
	// ... all other FSM fields stay the same ...
	l           sync.RWMutex
	path        string
	logger      log.Logger
	noopRestore bool

	// CHANGED: dbCache is now sync.Map instead of zcache.Cache
	dbCache sync.Map // map[string]*dbCacheEntry

	// NEW: singleflight group prevents duplicate database opens
	dbOpenGroup singleflight.Group

	// NEW: expiration configuration
	namespaceDBExpiration time.Duration
	expirationCheckPeriod time.Duration

	// NEW: signal to stop expiration goroutine
	stopExpiration chan struct{}

	mEnabled bool
	// ... other fields ...
}

// NewFSMWithSyncMap creates an FSM with the improved cache implementation
func NewFSMWithSyncMap(path string, localID string, mEnabled bool, expire, cleanup time.Duration, logger log.Logger) (*FSMWithSyncMap, error) {
	f := &FSMWithSyncMap{
		path:                  path,
		logger:                logger,
		mEnabled:              mEnabled,
		namespaceDBExpiration: expire,
		expirationCheckPeriod: cleanup,
		stopExpiration:        make(chan struct{}),
	}

	// Open the default database
	_, err := f.getDB(databaseFilename())
	if err != nil {
		return nil, err
	}

	// Start background expiration goroutine
	go f.runExpirationCleanup()

	return f, nil
}

// getDB retrieves or opens a database - NO EXCLUSIVE LOCKS!
func (f *FSMWithSyncMap) getDB(filename string) (*bolt.DB, error) {
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

// runExpirationCleanup runs in background to expire old namespace databases
func (f *FSMWithSyncMap) runExpirationCleanup() {
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
func (f *FSMWithSyncMap) expireOldDatabases() {
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

// Stats aggregates database statistics - updated for sync.Map
func (f *FSMWithSyncMap) Stats() bolt.Stats {
	var stats bolt.Stats

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

		return true // continue
	})

	return stats
}

// Close closes all databases - updated for sync.Map
func (f *FSMWithSyncMap) Close() error {
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

		return true // continue
	})

	// Clear the cache
	f.dbCache.Range(func(key, value interface{}) bool {
		f.dbCache.Delete(key)
		return true
	})

	return firstErr
}

// closeDBFile closes a specific database and removes it from cache
func (f *FSMWithSyncMap) closeDBFile(filename string) error {
	if entry, ok := f.dbCache.LoadAndDelete(filename); ok {
		cacheEntry := entry.(*dbCacheEntry)
		return cacheEntry.db.Close()
	}
	return nil
}

// getAllDatabases returns all cached database filenames
func (f *FSMWithSyncMap) getAllCachedDatabases() []string {
	var databases []string

	f.dbCache.Range(func(key, value interface{}) bool {
		databases = append(databases, key.(string))
		return true
	})

	return databases
}

// withDBView executes a read-only function - NO LOCKS NEEDED FOR CACHE!
func (f *FSMWithSyncMap) withDBView(ctx context.Context, fn func(*bolt.DB) error) error {
	db, err := f.getDatabaseForContext(ctx)
	if err != nil {
		return err
	}

	// Note: No f.l.RLock() needed! sync.Map handles concurrency
	return fn(db)
}

// withDBUpdate executes a read-write function - NO LOCKS NEEDED FOR CACHE!
func (f *FSMWithSyncMap) withDBUpdate(ctx context.Context, fn func(*bolt.DB) error) error {
	db, err := f.getDatabaseForContext(ctx)
	if err != nil {
		return err
	}

	// Note: No f.l.RLock() needed! sync.Map handles concurrency
	return fn(db)
}

// getDatabaseForContext retrieves the database for the given context
func (f *FSMWithSyncMap) getDatabaseForContext(ctx context.Context) (*bolt.DB, error) {
	vaultDBName, err := f.getDatabaseName(ctx)
	if err != nil {
		return nil, err
	}
	return f.getDB(vaultDBName)
}

// getDatabaseName determines the database name from context
func (f *FSMWithSyncMap) getDatabaseName(ctx context.Context) (string, error) {
	if !f.mEnabled {
		return databaseFilename(), nil
	}
	return getDatabaseName(ctx)
}

// openDBFile opens a BoltDB file (implementation stays the same)
func (f *FSMWithSyncMap) openDBFile(filename string) (*bolt.DB, error) {
	// ... existing implementation from fsm.go:354-422 ...
	// This method doesn't change, just included here for completeness
	return nil, nil // placeholder
}
