// Package standalone_storage implements a single-node storage engine for TinyKV.
// This storage engine uses BadgerDB as the underlying key-value store and supports
// Column Families (CF) through key prefixing.
//
// Architecture Overview:
// - Uses BadgerDB (LSM-tree based) for persistent storage
// - Implements Column Families by prefixing keys: "cf_key" format
// - Supports atomic batch operations via transactions
// - Provides read isolation through snapshot transactions
//
// Column Family Implementation:
// Since BadgerDB doesn't natively support Column Families, we simulate them
// by prefixing keys with the CF name and an underscore. For example:
// - CF="default", key="smith" becomes key="default_smith"
// - CF="write", key="smith" becomes key="write_smith"
//
// This allows different logical columns to coexist in the same physical database
// while maintaining isolation between them.
package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage implements the storage.Storage interface for single-node deployments.
// It provides a standalone key-value storage engine using BadgerDB with Column Family support.
//
// Thread Safety:
// - Safe for concurrent use (BadgerDB handles concurrency internally)
// - Each Reader creates its own transaction for isolation
// - Write operations use atomic transactions
//
// Lifecycle:
// 1. Create with NewStandAloneStorage()
// 2. Start() - Initialize (DB already opened in constructor)
// 3. Reader()/Write() - Perform operations
// 4. Stop() - Cleanup and close database
type StandAloneStorage struct {
	config *config.Config  // Configuration including DB path
	db     *badger.DB      // Underlying BadgerDB instance
}

// StandAloneReader implements storage.StorageReader interface.
// It provides read-only access to the database through a snapshot transaction.
// The transaction ensures consistent reads even while writes are happening.
//
// Usage Pattern:
// 1. Create via StandAloneStorage.Reader()
// 2. Perform read operations (GetCF, IterCF)
// 3. Always call Close() to release resources
type StandAloneReader struct {
	txn *badger.Txn  // Read-only transaction for consistent snapshots
}

// NewStandAloneStorage creates a new standalone storage instance.
//
// Parameters:
//   - conf: Configuration containing DB path and other settings
//
// Behavior:
//   - Opens BadgerDB at the specified path (creates directory if needed)
//   - Uses default BadgerDB options optimized for the TinyKV use case
//   - Panics if database cannot be opened (fatal error)
//
// Database Configuration:
//   - Dir: Database directory path from config
//   - ValueDir: Same as Dir (co-located with keys)
//   - Uses BadgerDB defaults for all other options
//
// Thread Safety:
//   - Safe to call from multiple goroutines
//   - Each call creates independent storage instance
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Configure BadgerDB options
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath

	// Open database (creates directory if needed)
	db, err := badger.Open(opts)
	if err != nil {
		panic(err) // Fatal error - cannot proceed without database
	}

	return &StandAloneStorage{
		config: conf,
		db:     db,
	}
}

// Start initializes the storage engine.
//
// Purpose:
//   - Ensures database is ready for operations
//   - Can be used to verify database health
//   - Provides initialization hook for future extensions
//
// Current Implementation:
//   - Database already opened in NewStandAloneStorage()
//   - This method acts as a verification/initialization checkpoint
//   - If DB is nil (edge case), opens it using configuration
//
// Returns:
//   - nil on success
//   - error if database cannot be opened
//
// Thread Safety:
//   - Safe to call multiple times
//   - Idempotent operation
func (s *StandAloneStorage) Start() error {
	// Handle edge case where DB might not be initialized
	if s.db == nil {
		opts := badger.DefaultOptions
		opts.Dir = s.config.DBPath
		opts.ValueDir = s.config.DBPath
		db, err := badger.Open(opts)
		if err != nil {
			return err
		}
		s.db = db
	}
	return nil
}

// Stop gracefully shuts down the storage engine.
//
// Purpose:
//   - Closes the underlying BadgerDB database
//   - Releases all database resources
//   - Ensures data persistence by closing cleanly
//
// Behavior:
//   - Safely handles nil database (idempotent)
//   - Blocks until database is fully closed
//   - All pending writes are persisted before closing
//
// Important Notes:
//   - After Stop(), no further operations are possible
//   - Must call NewStandAloneStorage() again to create new instance
//   - Database files remain on disk for future use
//
// Returns:
//   - nil on successful shutdown
//   - error if database close fails (data corruption possible)
func (s *StandAloneStorage) Stop() error {
	// Safely close database if it exists
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// GetCF retrieves a value from a specific Column Family.
//
// Parameters:
//   - cf: Column Family name (e.g., "default", "write", "lock")
//   - key: The key to retrieve (without CF prefix)
//
// Column Family Implementation:
//   - Automatically prefixes key with "cf_" format
//   - Example: GetCF("default", "smith") looks for key "default_smith"
//
// Return Values:
//   - value: The stored value as []byte
//   - nil: If key doesn't exist (not an error)
//   - error: Only for database errors (not for missing keys)
//
// Error Handling:
//   - Returns (nil, nil) for non-existent keys
//   - Returns (nil, error) for database errors
//   - Never returns empty []byte for missing keys
func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	// Create CF-prefixed key: "cf_key" format
	fullKey := engine_util.KeyWithCF(cf, key)

	// Retrieve item from transaction
	item, err := r.txn.Get(fullKey)
	if err != nil {
		// Handle missing key case
		if err == badger.ErrKeyNotFound {
			return nil, nil  // Key doesn't exist - not an error
		}
		return nil, err  // Database error
	}

	// Get value copy (safe to use outside transaction)
	val, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// IterCF creates an iterator for a specific Column Family.
//
// Parameters:
//   - cf: Column Family name (e.g., "default", "write", "lock")
//
// Return Value:
//   - Iterator that only returns keys/values from the specified CF
//   - Iterator must be closed by caller to release resources
//
// Iterator Behavior:
//   - Automatically filters to only show keys from specified CF
//   - Keys returned by iterator have CF prefix stripped
//   - Example: IterCF("default") will see keys stored as "default_*"
//   - Iterator.Key() returns original key (without "default_" prefix)
//
// Usage Pattern:
//   iter := reader.IterCF("default")
//   defer iter.Close()  // Always close!
//   for iter.Seek(startKey); iter.Valid(); iter.Next() {
//       key := iter.Item().Key()
//       value, err := iter.Item().Value()
//       // Process key/value...
//   }
//
// Resource Management:
//   - Caller MUST call Close() when done
//   - Iterator holds transaction resources until closed
//   - Multiple iterators can coexist on same transaction
func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	// Create CF-aware iterator that handles prefixing automatically
	return engine_util.NewCFIterator(cf, r.txn)
}

// Close releases resources held by the reader.
//
// Purpose:
//   - Discards the read-only transaction
//   - Releases memory and file descriptors
//   - Must be called to prevent resource leaks
//
// Behavior:
//   - Safe to call multiple times (idempotent)
//   - After Close(), reader becomes unusable
//   - Transaction resources are immediately released
//
// Important Notes:
//   - Failure to call Close() causes resource leaks
//   - Each Reader() call creates new transaction - must close each one
//   - Best practice: defer reader.Close() immediately after creation
//
// Example:
//   reader, err := storage.Reader(ctx)
//   if err != nil {
//       return err
//   }
//   defer reader.Close()  // Always close!
//   // Use reader...
func (r *StandAloneReader) Close() {
	// Discard transaction to release resources
	// Safe to call multiple times - second call is no-op
	r.txn.Discard()
}

// Reader creates a new StorageReader for read operations.
//
// Purpose:
//   - Provides consistent snapshot of database at transaction start time
//   - Enables multiple read operations with same view of data
//   - Supports both point lookups (GetCF) and range scans (IterCF)
//
// Transaction Properties:
//   - Creates read-only transaction (false parameter)
//   - Provides snapshot isolation - sees consistent view of database
//   - Other writes can proceed concurrently without affecting this reader
//
// Column Family Support:
//   - Reader works with all Column Families
//   - CF specified per operation (GetCF, IterCF)
//   - Same reader can access multiple CFs
//
// Resource Management:
//   - Caller MUST call Close() on returned reader
//   - Best practice: defer reader.Close() immediately after creation
//   - Each call creates new transaction - independent snapshots
//
// Parameters:
//   - ctx: Request context (currently unused but reserved for future)
//
// Returns:
//   - StorageReader interface for read operations
//   - error if transaction cannot be created (rare)
//
// Example:
//   reader, err := storage.Reader(ctx)
//   if err != nil {
//       return err
//   }
//   defer reader.Close()  // Always close!
//
//   value, err := reader.GetCF("default", "key")
//   iter := reader.IterCF("default")
//   // Use reader...
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Create read-only transaction for consistent snapshot
	// false = read-only, true = writable
	txn := s.db.NewTransaction(false)

	// Return reader that implements StorageReader interface
	return &StandAloneReader{txn: txn}, nil
}

// Write performs a batch of atomic write operations.
//
// Purpose:
//   - Applies multiple modifications atomically (all-or-nothing)
//   - Supports both Put (insert/update) and Delete operations
//   - Provides ACID properties through BadgerDB transactions
//
// Atomicity Guarantee:
//   - All operations in batch succeed together, or all fail together
//   - If any operation fails, entire batch is rolled back
//   - Other readers see either all changes or none of them
//
// Supported Operations:
//   - storage.Put: Insert or update key-value pair
//   - storage.Delete: Remove key (and its value)
//
// Column Family Support:
//   - Each operation specifies its Column Family
//   - Different CFs can be mixed in same batch
//   - CF prefixing handled automatically via engine_util.KeyWithCF()
//
// Parameters:
//   - ctx: Request context (currently unused but reserved for future)
//   - batch: Slice of Modify operations to apply atomically
//
// Returns:
//   - nil: All operations applied successfully
//   - error: Any operation failed, entire batch rolled back
//
// Performance Notes:
//   - Single transaction for entire batch (efficient)
//   - Operations executed in order provided
//   - Large batches should be chunked for better performance
//
// Example:
//   batch := []storage.Modify{
//       {Data: storage.Put{Cf: "default", Key: []byte("key1"), Value: []byte("value1")}},
//       {Data: storage.Delete{Cf: "write", Key: []byte("key2")}},
//   }
//   err := storage.Write(ctx, batch)
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, modify := range batch {
			switch data := modify.Data.(type) {
			case storage.Put:
				fullKey := engine_util.KeyWithCF(data.Cf, data.Key)
				if err := txn.Set(fullKey, data.Value); err != nil {
					return err // Will rollback entire transaction
				}
			case storage.Delete:
				fullKey := engine_util.KeyWithCF(data.Cf, data.Key)
				if err := txn.Delete(fullKey); err != nil {
					return err // Will rollback entire transaction
				}
			}
		}
		return nil // Will commit all changes
	})
}
