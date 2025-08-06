# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] - v0.2.0

### Added
- **Sharding support**: Group items by custom keys using `KeyFunc` for intelligent batch formation
- **Independent per-shard timers**: Each shard now has its own timer, allowing for true independent batching behavior across different keys
- **Memory management**: Automatic LRU cleanup for inactive batch keys to prevent memory leaks
- **Configurable cleanup**: Customizable thresholds for cleanup frequency and key inactivity
- **Sharding examples**: Complete example demonstrating experiment-based grouping

### Changed
- **Trigger-based architecture**: Moved from ticker-based polling to event-driven trigger channels for better responsiveness
- **BatchManager responsibility**: Centralized batch lifecycle management including timer coordination and size-based triggering
- **API improvement**: `maxBatchSize` is now a struct property instead of being passed to every method call

### Fixed
- **Panic handling cleanup**: Simplified overly defensive panic recovery code, making it more readable and maintainable
- **Ticker reset issue**: Fixed ticker not being reset after size-based batch processing

## [v0.1.1] - 2025-05-17

### Changed
- **Performance optimization**: Removed unnecessary copying of batch items before processing
- **Memory efficiency**: Direct processing of items without intermediate copies

### Added
- **Version documentation**: Added `v0.MD` with detailed version history

## [v0.1.0] - 2025-05-17

### Added
- **Initial release**: Core batch processing functionality with Go 1.18+ generics
- **Concurrent processing**: Configurable worker pool for parallel batch execution
- **Dual-trigger batching**: Process batches based on size limits or time intervals
- **Panic safety**: Recover from panics in user-provided batch functions
- **Graceful shutdown**: Ensure all queued items are processed before stopping
- **ID correlation**: Map results back to specific input items using unique IDs
- **Synchronous API**: `SubmitAndAwait` method for easy integration
- **Comprehensive testing**: Full test suite with edge cases and concurrency scenarios
- **Documentation**: Complete README with usage examples and API documentation
- **Example application**: Simple user profile fetching demonstration

### Technical Features
- Generic types support (`T` input, `Q` output)
- Buffered channels for non-blocking operations
- Worker pool semaphore for concurrency control
- Ticker-based time intervals
- Error propagation and handling
- Thread-safe operations

---

## Version History Summary

- **v0.1.0**: Foundation - Basic batch processing with concurrency
- **v0.1.1**: Performance - Removed unnecessary copying overhead  
- **v0.2.0** *(Unreleased)*: Intelligence & Efficiency - Sharding, per-shard timers, and trigger architecture

## Migration Guide

### From v0.1.x to v0.2.0 *(Unreleased)*
- No breaking changes to existing API
- Optional sharding can be enabled by providing `KeyFunc` in config
- Memory management is automatically enabled for sharded mode
- Improved performance with per-shard timers and trigger-based architecture
