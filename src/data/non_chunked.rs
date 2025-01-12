//! This is code responsible for *non-chunked* data
//! corresponding to times in a [`crate::time::OffsetTimeList`].
//! AKA, this is just one giant timespan with no breaks.

/// A struct representing data that will not have any breaks;
/// if you use this, you are assuming each time will have a
/// corresponding value.
pub struct NonChunkedData<T>(Vec<T>);
