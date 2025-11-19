// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	log "github.com/hashicorp/go-hclog"
	bolt "go.etcd.io/bbolt"
)

// TestCacheOperationNoBlocking verifies that cache misses don't block other operations
func TestCacheOperationNoBlocking(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create FSM with sync.Map
	f := &FSMWithSyncMap{
		path:                  tmpDir,
		logger:                log.NewNullLogger(),
		mEnabled:              true,
		namespaceDBExpiration: 1 * time.Hour,
		expirationCheckPeriod: 5 * time.Minute,
		stopExpiration:        make(chan struct{}),
	}

	// Create multiple database files
	for i := 0; i < 5; i++ {
		dbPath := filepath.Join(tmpDir, fmt.Sprintf("db%d.db", i))
		db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
		if err != nil {
			t.Fatalf("failed to create test database: %v", err)
		}
		db.Close()
	}

	var wg sync.WaitGroup
	var blockedCount atomic.Int32
	var successCount atomic.Int32

	// Launch 100 goroutines trying to open databases concurrently
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Each goroutine opens a different database
			dbName := fmt.Sprintf("db%d.db", id%5)

			start := time.Now()
			db, err := f.getDB(dbName)
			elapsed := time.Since(start)

			if err != nil {
				t.Errorf("goroutine %d failed to open %s: %v", id, dbName, err)
				return
			}

			if db == nil {
				t.Errorf("goroutine %d got nil database", id)
				return
			}

			// If operation took more than 100ms, consider it blocked
			// (file open should be fast, blocking would make it slow)
			if elapsed > 100*time.Millisecond {
				blockedCount.Add(1)
			} else {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()
	f.Close()

	t.Logf("Success: %d, Blocked: %d", successCount.Load(), blockedCount.Load())

	// With sync.Map, most operations should be fast (not blocked)
	// Allow some blocking due to singleflight coordination, but should be <20%
	if blockedCount.Load() > 20 {
		t.Errorf("Too many operations blocked: %d (expected <20)", blockedCount.Load())
	}
}

// TestSingleflightPreventsDuplicateOpens verifies singleflight prevents duplicate opens
func TestSingleflightPreventsDuplicateOpens(t *testing.T) {
	tmpDir := t.TempDir()

	f := &FSMWithSyncMap{
		path:                  tmpDir,
		logger:                log.NewNullLogger(),
		mEnabled:              true,
		namespaceDBExpiration: 1 * time.Hour,
		expirationCheckPeriod: 5 * time.Minute,
		stopExpiration:        make(chan struct{}),
	}

	// Create test database
	dbName := "test.db"
	dbPath := filepath.Join(tmpDir, dbName)
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}
	db.Close()

	// Track how many times openDBFile is called
	var openCount atomic.Int32
	originalOpen := f.openDBFile
	f.openDBFile = func(filename string) (*bolt.DB, error) {
		openCount.Add(1)
		time.Sleep(50 * time.Millisecond) // Simulate slow open
		return originalOpen(filename)
	}

	// Launch 10 goroutines trying to open the same database simultaneously
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.getDB(dbName)
			if err != nil {
				t.Errorf("failed to get database: %v", err)
			}
		}()
	}

	wg.Wait()
	f.Close()

	// With singleflight, openDBFile should only be called ONCE
	if openCount.Load() != 1 {
		t.Errorf("openDBFile called %d times, expected 1 (singleflight should deduplicate)", openCount.Load())
	}
}

// TestCacheExpiration verifies namespace databases expire correctly
func TestCacheExpiration(t *testing.T) {
	tmpDir := t.TempDir()

	f := &FSMWithSyncMap{
		path:                  tmpDir,
		logger:                log.New(&log.LoggerOptions{Level: log.Debug}),
		mEnabled:              true,
		namespaceDBExpiration: 100 * time.Millisecond, // Short expiration for testing
		expirationCheckPeriod: 50 * time.Millisecond,  // Check frequently
		stopExpiration:        make(chan struct{}),
	}

	// Create namespace database
	nsDBName := "namespace.db"
	nsDBPath := filepath.Join(tmpDir, nsDBName)
	db, err := bolt.Open(nsDBPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		t.Fatalf("failed to create namespace database: %v", err)
	}
	db.Close()

	// Open namespace database
	_, err = f.getDB(nsDBName)
	if err != nil {
		t.Fatalf("failed to open namespace database: %v", err)
	}

	// Verify it's cached
	if _, ok := f.dbCache.Load(nsDBName); !ok {
		t.Fatal("namespace database should be cached")
	}

	// Start expiration goroutine
	go f.runExpirationCleanup()

	// Wait for expiration (100ms expiry + 50ms check = ~200ms)
	time.Sleep(250 * time.Millisecond)

	// Verify it's no longer cached
	if _, ok := f.dbCache.Load(nsDBName); ok {
		t.Error("namespace database should have expired from cache")
	}

	f.Close()
}

// TestDefaultDatabaseNeverExpires verifies default database doesn't expire
func TestDefaultDatabaseNeverExpires(t *testing.T) {
	tmpDir := t.TempDir()

	f := &FSMWithSyncMap{
		path:                  tmpDir,
		logger:                log.NewNullLogger(),
		mEnabled:              true,
		namespaceDBExpiration: 100 * time.Millisecond,
		expirationCheckPeriod: 50 * time.Millisecond,
		stopExpiration:        make(chan struct{}),
	}

	// Create default database
	defaultDB := databaseFilename()
	defaultPath := filepath.Join(tmpDir, defaultDB)
	db, err := bolt.Open(defaultPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		t.Fatalf("failed to create default database: %v", err)
	}
	db.Close()

	// Open default database
	_, err = f.getDB(defaultDB)
	if err != nil {
		t.Fatalf("failed to open default database: %v", err)
	}

	// Start expiration goroutine
	go f.runExpirationCleanup()

	// Wait longer than expiration time
	time.Sleep(250 * time.Millisecond)

	// Verify default database is still cached
	if _, ok := f.dbCache.Load(defaultDB); !ok {
		t.Error("default database should never expire from cache")
	}

	f.Close()
}

// TestConcurrentReadsDifferentDatabases verifies reads don't block each other
func TestConcurrentReadsDifferentDatabases(t *testing.T) {
	tmpDir := t.TempDir()

	f := &FSMWithSyncMap{
		path:                  tmpDir,
		logger:                log.NewNullLogger(),
		mEnabled:              true,
		namespaceDBExpiration: 1 * time.Hour,
		expirationCheckPeriod: 5 * time.Minute,
		stopExpiration:        make(chan struct{}),
	}

	// Create multiple databases with test data
	numDBs := 5
	for i := 0; i < numDBs; i++ {
		dbName := fmt.Sprintf("db%d.db", i)
		dbPath := filepath.Join(tmpDir, dbName)

		db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
		if err != nil {
			t.Fatalf("failed to create database %s: %v", dbName, err)
		}

		// Write test data
		db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("test"))
			if err != nil {
				return err
			}
			return b.Put([]byte("key"), []byte(fmt.Sprintf("value%d", i)))
		})

		db.Close()
	}

	// Perform concurrent reads from different databases
	var wg sync.WaitGroup
	var readCount atomic.Int32
	start := time.Now()

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			dbName := fmt.Sprintf("db%d.db", id%numDBs)
			db, err := f.getDB(dbName)
			if err != nil {
				t.Errorf("failed to get database: %v", err)
				return
			}

			// Perform read operation
			err = db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("test"))
				if b == nil {
					return fmt.Errorf("bucket not found")
				}
				val := b.Get([]byte("key"))
				if val == nil {
					return fmt.Errorf("key not found")
				}
				return nil
			})

			if err != nil {
				t.Errorf("read failed: %v", err)
				return
			}

			readCount.Add(1)
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	f.Close()

	t.Logf("Completed %d reads in %v", readCount.Load(), elapsed)

	// All 100 reads should succeed
	if readCount.Load() != 100 {
		t.Errorf("Expected 100 successful reads, got %d", readCount.Load())
	}

	// With no blocking, 100 reads should be very fast (<500ms)
	if elapsed > 500*time.Millisecond {
		t.Errorf("Reads took too long (%v), may indicate blocking", elapsed)
	}
}

// BenchmarkGetDBCacheHit benchmarks cache hit performance
func BenchmarkGetDBCacheHit(b *testing.B) {
	tmpDir := b.TempDir()

	f := &FSMWithSyncMap{
		path:                  tmpDir,
		logger:                log.NewNullLogger(),
		mEnabled:              true,
		namespaceDBExpiration: 1 * time.Hour,
		expirationCheckPeriod: 5 * time.Minute,
		stopExpiration:        make(chan struct{}),
	}

	// Create and cache database
	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		b.Fatalf("failed to create database: %v", err)
	}
	db.Close()

	// Warm up cache
	f.getDB("test.db")

	b.ResetTimer()

	// Benchmark cache hits (should be extremely fast with sync.Map)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := f.getDB("test.db")
			if err != nil {
				b.Fatalf("getDB failed: %v", err)
			}
		}
	})

	f.Close()
}

// BenchmarkGetDBCacheMiss benchmarks cache miss performance
func BenchmarkGetDBCacheMiss(b *testing.B) {
	tmpDir := b.TempDir()

	f := &FSMWithSyncMap{
		path:                  tmpDir,
		logger:                log.NewNullLogger(),
		mEnabled:              true,
		namespaceDBExpiration: 1 * time.Hour,
		expirationCheckPeriod: 5 * time.Minute,
		stopExpiration:        make(chan struct{}),
	}

	// Create database files but don't cache them
	for i := 0; i < b.N; i++ {
		dbPath := filepath.Join(tmpDir, fmt.Sprintf("db%d.db", i))
		db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
		if err != nil {
			b.Fatalf("failed to create database: %v", err)
		}
		db.Close()
	}

	b.ResetTimer()

	// Benchmark cache misses
	for i := 0; i < b.N; i++ {
		dbName := fmt.Sprintf("db%d.db", i)
		_, err := f.getDB(dbName)
		if err != nil {
			b.Fatalf("getDB failed: %v", err)
		}
	}

	f.Close()
}
