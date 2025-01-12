//! Code around the time aspect. Stored as a list of offsets,
//! each a negative offset of the next value, with the latest
//! value being represented in whole.

use std::time::{Duration, Instant};

/// Time stored as a bunch of offsets.
#[derive(Default, Clone, Debug)]
pub struct OffsetTimeList {
    time_offsets: Vec<u32>,
    checkpoints: Vec<(Instant, usize)>,
    current_time: Option<Instant>,
}

impl OffsetTimeList {
    /// Create a [`OffsetTimeList`] with a capacity pre-initialized.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_both_capacity(capacity, 0)
    }

    /// Create a [`OffsetTimeList`] with both time storage and
    /// checkpoint capacity pre-initialized.
    pub fn with_both_capacity(time_capacity: usize, checkpoint_capacity: usize) -> Self {
        Self {
            time_offsets: Vec::with_capacity(time_capacity),
            checkpoints: Vec::with_capacity(checkpoint_capacity),
            current_time: None,
        }
    }

    /// Add a time entry. This will return the current index,
    /// which can be used to update any [`crate::data::Data`] entries
    /// that are corresponding to this [`OffsetTimeList`].
    pub fn add(&mut self, time: Instant) -> usize {
        if let Some(current_time) = self.current_time {
            let offset = time.duration_since(current_time).as_millis() as u32;
            self.current_time = Some(time);
            self.time_offsets.push(offset);

            // The current "index" is the length of the vec - 1, but we
            // add back 1 since we store the current head as a separate instant.
            self.time_offsets.len()
        } else {
            self.current_time = Some(time);

            1
        }
    }

    /// Add a "checkpoint"; this is used for pruning by time.
    pub fn checkpoint(&mut self) {
        if let Some(current_time) = self.current_time {
            self.checkpoints
                .push((current_time, self.time_offsets.len()));
        }
    }

    /// Approximately prune time values older than the given [`Duration`],
    /// and returns the new index.
    pub fn prune(&mut self, max_age: Duration) -> Option<usize> {
        if let Some(current_time) = self.current_time {
            let checkpoint_index = match self.checkpoints.binary_search_by(|(instant, _)| {
                println!(
                    "current time duration since: {:?}",
                    current_time.duration_since(*instant)
                );
                current_time.duration_since(*instant).cmp(&max_age)
            }) {
                Ok(index) | Err(index) => index,
            };

            let checkpoint_index =
                std::cmp::min(checkpoint_index, self.checkpoints.len().saturating_sub(1));

            match self.checkpoints.drain(..checkpoint_index).last() {
                Some((_, index)) => {
                    if index < self.time_offsets.len() {
                        self.time_offsets.drain(..index);
                        Some(self.time_offsets.len())
                    } else {
                        self.time_offsets.clear();
                        self.current_time = None;
                        Some(0)
                    }
                }
                None => None,
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        let mut times = OffsetTimeList::default();

        let now = Instant::now();
        times.add(now);

        assert_eq!(times.current_time, Some(now));

        let next = now + Duration::from_millis(1);
        times.add(next);

        assert_eq!(times.current_time, Some(next));
        assert_eq!(times.time_offsets, vec!(1));
    }

    #[test]
    fn test_prune() {
        let mut times = OffsetTimeList::default();

        // Test fully empty.
        assert_eq!(times.prune(Duration::from_secs(0)), None);

        let now = Instant::now();
        times.add(now);

        // Test no checkpoint.
        assert_eq!(times.prune(Duration::from_secs(0)), None);

        // Add a checkpoint, try clearing it.
        times.add(now);
        times.checkpoint();

        assert_eq!(times.prune(Duration::from_secs(1000)), Some(1));
        assert_eq!(times.prune(Duration::from_secs(0)), None);
    }
}
