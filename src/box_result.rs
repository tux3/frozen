use std::error::Error;

pub type BoxError = Box<dyn Error + Send + Sync + 'static>;
pub type BoxResult<T> = Result<T, BoxError>;
