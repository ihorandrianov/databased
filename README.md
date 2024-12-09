# databased

A persistent key-value store implemented in Rust with Write-Ahead Logging (WAL) for durability. The project implements a multi-layer architecture combining in-memory operations with disk persistence, LRU caching, and block-based storage.

## Project Status

This is a work in progress. Current TODO list:
* Refactor CRC logic to avoid repetition
* Abstract common parts of serialization process
* Add edge case handling for out-of-bound access
* Improve error handling and messages
* Optimize CRC computation
* Add more comprehensive testing
* Implement overflow handling in varint conversion
* Add documentation

## Codebase Overview

### Key components:

* `bytecode_serializer.rs` - Handles serialization of operations into bytecode format
* `kvstore.rs` - Main store implementation coordinating between different layers
* `lru_cache.rs` - LRU caching implementation
* `log.rs` - Write-Ahead Logging implementation
* `operation.rs` - Defines store operations (SET/GET/DEL)
* `parser.rs` - Command parsing logic
* `block.rs` - Block-based storage format implementation

### Storage is organized in layers:
* In-memory layer for fast operations
* LRU cache for frequently accessed data
* WAL for durability
* Block-based persistent storage

## Basic Usage

The store accepts three basic operations:

```shell
SET key TO value
GET key
DEL key
```

Commands can be chained using AND:

```shell
SET key1 TO value1 AND GET key1 AND DEL key1
```

## Development

```shell
# Run the project
cargo run

# Run tests
cargo test
Copy
```
