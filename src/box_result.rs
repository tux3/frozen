use std::error::Error;

pub type BoxResult<T> = Result<T, Box<dyn Error + Send + Sync + 'static>>;
