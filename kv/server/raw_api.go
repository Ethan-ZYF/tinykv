// Package server implements the TinyKV server with Raw API support.
//
// Raw API provides client-facing key-value operations that directly manipulate
// the underlying storage engine. These operations are "raw" because they bypass
// any transaction processing or conflict resolution - they directly read/write
// to the storage engine.
//
// Raw API Operations:
//   - RawGet: Retrieve a single key-value pair
//   - RawPut: Store a single key-value pair
//   - RawDelete: Remove a single key
//   - RawScan: Scan multiple key-value pairs with limit
//
// Architecture:
//   - Each operation creates a storage.Reader or uses storage.Write
//   - All operations support Column Family isolation
//   - Operations are atomic at the individual request level
//   - No multi-operation transactions in Raw API
//
// Error Handling:
//   - Returns errors for system failures
//   - Uses NotFound flag for missing keys (not errors)
//   - All operations must handle resource cleanup
package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory
//
// Raw API Design Principles:
//   1. Simplicity: Direct mapping to storage operations
//   2. Consistency: All operations support Column Families
//   3. Reliability: Proper error handling and resource cleanup
//   4. Performance: Minimal overhead over storage operations

// RawGet retrieves a single key-value pair from the specified Column Family.
//
// Parameters:
//   - req.Cf: Column Family name (e.g., "default", "write", "lock")
//   - req.Key: The key to retrieve (without CF prefix)
//
// Behavior:
//   - Creates a storage.Reader for consistent snapshot isolation
//   - Uses reader.GetCF() to retrieve the value
//   - Properly handles missing keys (not an error condition)
//   - Always releases resources via defer reader.Close()
//
// Response:
//   - Value: The stored value as []byte (nil if key not found)
//   - NotFound: Boolean flag indicating if key was not found
//
// Error Handling:
//   - Returns error only for system failures (storage issues)
//   - Missing keys are indicated via NotFound flag, not errors
//
// Example Usage:
//   resp, err := server.RawGet(ctx, &kvrpcpb.RawGetRequest{
//       Cf:  "default",
//       Key: []byte("user:123"),
//   })
//   if err != nil {
//       return err  // System error
//   }
//   if resp.NotFound {
//       // Handle missing key
//   } else {
//       value := resp.Value  // Process value
//   }
//
// Implementation Notes:
//   - Uses snapshot isolation for consistent reads
//   - Column Family prefixing handled automatically by storage layer
//   - Resource cleanup guaranteed by defer statement
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Create a storage reader for consistent snapshot isolation
	// nil context is acceptable for raw operations (no transaction context needed)
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close() // Ensure resources are released

	// Retrieve the value from the specified Column Family
	// GetCF handles Column Family prefixing automatically
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err // Propagate storage errors
	}

	// Construct response with proper NotFound flag
	// NotFound = true when value is nil (key doesn't exist)
	// NotFound = false when value exists (even if empty)
	return &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: value == nil,
	}, nil
}

// RawPut stores a single key-value pair in the specified Column Family.
//
// Parameters:
//   - req.Cf: Column Family name (e.g., "default", "write", "lock")
//   - req.Key: The key to store (without CF prefix)
//   - req.Value: The value to store
//
// Behavior:
//   - Creates a storage.Put operation
//   - Wraps in storage.Modify for batch compatibility
//   - Uses storage.Write() for atomic persistence
//   - Overwrites existing values (insert or update semantics)
//
// Atomicity:
//   - Single operation is atomic (all-or-nothing)
//   - Uses underlying storage transaction for consistency
//   - Other operations see either old or new value, never partial
//
// Response:
//   - Empty RawPutResponse on success
//   - Error on system failure (storage issues)
//
// Error Handling:
//   - Returns error only for system failures
//   - Success means value is durably stored
//   - No conflict detection (raw operations)
//
// Example Usage:
//   _, err := server.RawPut(ctx, &kvrpcpb.RawPutRequest{
//       Cf:    "default",
//       Key:   []byte("user:123"),
//       Value: []byte("John Doe"),
//   })
//   if err != nil {
//       return err  // System error
//   }
//   // Value is now durably stored
//
// Implementation Notes:
//   - Uses single-operation batch for atomicity
//   - Column Family prefixing handled automatically
//   - No resource cleanup needed (Write manages its own transaction)
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Create a Put operation specifying the key, value, and Column Family
	// This represents the data modification we want to apply
	put := storage.Put{
		Key:   req.GetKey(),   // Key to store (without CF prefix)
		Value: req.GetValue(), // Value to store
		Cf:    req.GetCf(),    // Column Family (e.g., "default", "write", "lock")
	}

	// Wrap the Put operation in a Modify struct
	// This allows the operation to be part of a batch (even single operations)
	// The Data field can hold either Put or Delete operations
	modify := storage.Modify{Data: put}

	// Use storage.Write() to atomically apply the modification
	// nil context is acceptable for raw operations
	// Single-operation batch ensures atomicity
	err := server.storage.Write(nil, []storage.Modify{modify})
	if err != nil {
		return nil, err // Propagate storage errors (system failures)
	}

	// Return empty response on success
	// RawPut has no response data - success means value was stored
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete removes a single key from the specified Column Family.
//
// Parameters:
//   - req.Cf: Column Family name (e.g., "default", "write", "lock")
//   - req.Key: The key to delete (without CF prefix)
//
// Behavior:
//   - Creates a storage.Delete operation
//   - Wraps in storage.Modify for batch compatibility
//   - Uses storage.Write() for atomic deletion
//   - Idempotent: deleting non-existent key is not an error
//
// Atomicity:
//   - Single operation is atomic (all-or-nothing)
//   - Uses underlying storage transaction for consistency
//   - Other operations see either before or after state
//
// Response:
//   - Empty RawDeleteResponse on success
//   - Error on system failure (storage issues)
//
// Error Handling:
//   - Returns error only for system failures
//   - Success means key was deleted (or didn't exist)
//   - No error for deleting non-existent keys
//
// Example Usage:
//   _, err := server.RawDelete(ctx, &kvrpcpb.RawDeleteRequest{
//       Cf:  "default",
//       Key: []byte("user:123"),
//   })
//   if err != nil {
//       return err  // System error
//   }
//   // Key is now deleted (or didn't exist)
//
// Implementation Notes:
//   - Uses single-operation batch for atomicity
//   - Column Family prefixing handled automatically
//   - No resource cleanup needed (Write manages its own transaction)
//   - Idempotent operation - safe to call multiple times
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Create a Delete operation specifying the key and Column Family
	// This represents the deletion we want to apply
	delete := storage.Delete{
		Key: req.GetKey(), // Key to delete (without CF prefix)
		Cf:  req.GetCf(),  // Column Family (e.g., "default", "write", "lock")
	}

	// Wrap the Delete operation in a Modify struct
	// This allows the operation to be part of a batch (even single operations)
	// The Data field can hold either Put or Delete operations
	modify := storage.Modify{Data: delete}

	// Use storage.Write() to atomically apply the deletion
	// nil context is acceptable for raw operations
	// Single-operation batch ensures atomicity
	err := server.storage.Write(nil, []storage.Modify{modify})
	if err != nil {
		return nil, err // Propagate storage errors (system failures)
	}

	// Return empty response on success
	// RawDelete has no response data - success means key was deleted
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan retrieves multiple key-value pairs from the specified Column Family,
// starting from a given key and returning up to a specified limit of results.
//
// Parameters:
//   - req.Cf: Column Family name (e.g., "default", "write", "lock")
//   - req.StartKey: The key to start scanning from (inclusive)
//   - req.Limit: Maximum number of key-value pairs to return
//
// Behavior:
//   - Creates a storage.Reader for consistent snapshot isolation
//   - Uses reader.IterCF() to create Column Family iterator
//   - Seeks to start key and iterates forward
//   - Collects up to 'limit' key-value pairs
//   - Always releases resources via defer statements
//
// Scan Semantics:
//   - Starts at or after req.StartKey (inclusive)
//   - Returns results in key-sorted order
//   - Stops when limit reached or no more keys
//   - Empty result set is valid (no error)
//
// Response:
//   - Array of KvPair structs containing key-value pairs
//   - Empty array if no keys found in range
//   - Maximum length = req.Limit
//
// Error Handling:
//   - Returns error only for system failures (storage issues)
//   - Empty scan results are not errors
//
// Example Usage:
//   resp, err := server.RawScan(ctx, &kvrpcpb.RawScanRequest{
//       Cf:       "default",
//       StartKey: []byte("user:100"),
//       Limit:    10,
//   })
//   if err != nil {
//       return err  // System error
//   }
//   for _, kv := range resp.Kvs {
//       key := kv.Key    // Original key (CF prefix stripped)
//       value := kv.Value // Stored value
//       // Process key-value pair...
//   }
//
// Implementation Notes:
//   - Uses snapshot isolation for consistent scans
//   - Iterator automatically strips Column Family prefixes
//   - Resource cleanup for both reader and iterator
//   - Efficient iteration with early termination at limit
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Create a storage reader for consistent snapshot isolation
	// nil context is acceptable for raw operations
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close() // Ensure reader resources are released

	// Create iterator for the specified Column Family
	// IterCF automatically handles CF prefixing and filtering
	iter := reader.IterCF(req.GetCf())
	defer iter.Close() // Ensure iterator resources are released

	// Prepare response array for key-value pairs
	// Pre-allocate with reasonable capacity if limit is reasonable
	var kvs []*kvrpcpb.KvPair

	// Iterate through keys starting from start_key, up to limit
	// Seek positions iterator at or after start key
	// Valid checks if current position is valid
	// Next advances to next key
	count := uint32(0)
	for iter.Seek(req.GetStartKey()); iter.Valid() && count < req.GetLimit(); iter.Next() {
		item := iter.Item()

		// Get key (iterator automatically strips CF prefix)
		// KeyCopy creates a safe copy that can be used after iterator closes
		key := item.KeyCopy(nil)

		// Get value (safe copy that can be used after iterator closes)
		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err // Propagate storage errors
		}

		// Add key-value pair to results
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})

		count++
	}

	// Return scan results
	// Kvs array contains up to 'limit' key-value pairs in sorted order
	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
