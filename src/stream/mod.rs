mod compression_stream;
pub use compression_stream::*;
mod encryption_stream;
pub use encryption_stream::*;
mod hashed_stream;
pub use hashed_stream::*;
mod simple_bytes_stream;
pub use simple_bytes_stream::*;

/// Size of a byte stream's chunks (must be above B2's 5MB minimum part size)
pub const STREAMS_CHUNK_SIZE: usize = 16 * 1024 * 1024;
/// Max pending chunks that a stream will buffer
pub const CHUNK_BUFFER_COUNT: usize = 4;
