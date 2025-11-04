## 1. Understand the Architecture
- Column Family (CF): Uses key prefixes like cf_key to simulate multiple columns
- Badger: LSM-tree based KV storage (WiscKey design)
- Storage Interface: Core interface in kv/storage/storage.go with 4 methods
- Modify Struct: Wrapper for Put/Delete operations in kv/storage/modify.go

## 2. Key Components You Need to Implement
A. StandAloneStorage (kv/storage/standalone_storage/standalone_storage.go)
- Fields: Add db *badger.DB field to store badger instance
- NewStandAloneStorage: Create badger DB using conf.DBPath
- Start/Stop: Initialize and close badger DB
- Write: Process batch of Modify operations in single transaction
- Reader: Return custom StorageReader implementation

B. StorageReader Implementation
Create a struct that implements:
- GetCF(cf, key): Get value from specific column family
- IterCF(cf): Create iterator for column family
- Close(): Clean up resources

C. Raw API Handlers (kv/server/raw_api.go)
- RawGet: Get value, set NotFound=true if missing
- RawPut: Convert to Modify and write
- RawDelete: Convert to Modify and write
- RawScan: Iterate with limit, starting from StartKey

## 3. Implementation Order

Step 1: Implement standalone_storage.go
1. Add badger DB field to struct
2. Implement NewStandAloneStorage to create badger instance
3. Implement Start() and Stop() for DB lifecycle
4. Implement Write() method for batch operations
5. Create inner Reader struct and implement Reader() method

Step 2: Implement raw_api.go
1. Implement RawGet - handle not found case
2. Implement RawPut - use Modify wrapper
3. Implement RawDelete - use Modify wrapper
4. Implement RawScan - iterate with limit

## 4. Key Helper Functions Available
- engine_util.KeyWithCF(cf, key) - Add CF prefix to key
- engine_util.GetCFFromTxn(txn, cf, key) - Get value from transaction
- engine_util.NewCFIterator(cf, txn) - Create CF iterator
- engine_util.PutCF/DeleteCF - Direct DB operations

## 5. Testing Strategy
After implementation, you should test:
- Basic Put/Get operations
- Column family isolation
- Batch writes
- Scan operations with limits
- Delete operations
- Not found scenarios