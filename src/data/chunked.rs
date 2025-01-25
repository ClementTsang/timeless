//! This is code responsible for possibly chunked data.

#[derive(Clone, Default, Debug)]
struct DataChunk<T> {
    /// The start offset of this chunk, should correspond to the time vector
    /// indices. If that updates, this MUST also update.
    start_offset: usize,

    /// The actual value data!
    data: Vec<T>,
}

impl<T> DataChunk<T> {
    fn push(&mut self, item: T) {
        self.data.push(item)
    }
}

/// An iterator created from a [`ChunkedData`].
pub struct ChunkedDataIter<I: Iterator + DoubleEndedIterator> {
    iter: I,
    size: usize,
}

impl<T, I: Iterator<Item = T> + DoubleEndedIterator> Iterator for ChunkedDataIter<I> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<T, I: Iterator<Item = T> + DoubleEndedIterator> ExactSizeIterator for ChunkedDataIter<I> {
    fn len(&self) -> usize {
        self.size
    }
}

impl<T, I: Iterator<Item = T> + DoubleEndedIterator> DoubleEndedIterator for ChunkedDataIter<I> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}

/// A struct representing data that may potentially have breaks.
/// If you expect that you may want to store time values but _not_
/// data values, use this to avoid storing blanks.
#[derive(Clone, Default, Debug)]
pub struct ChunkedData<D> {
    next_index: usize,
    is_active: bool,
    chunks: Vec<DataChunk<D>>,
}

impl<D> ChunkedData<D> {
    /// Returns an iterator of items alongside the associated indices for each item.
    pub fn iter_with_index(&self) -> ChunkedDataIter<impl DoubleEndedIterator<Item = (usize, &D)>> {
        let size = self.chunks.iter().map(|dc| dc.data.len()).sum();
        let iter = self.chunks.iter().flat_map(|dc| {
            let start = dc.start_offset;

            dc.data
                .iter()
                .enumerate()
                .map(move |(offset, datum)| (start + offset, datum))
        });

        ChunkedDataIter { iter, size }
    }

    /// Returns an iterator of items.
    pub fn iter(&self) -> ChunkedDataIter<impl DoubleEndedIterator<Item = &D>> {
        let size = self.chunks.iter().map(|dc| dc.data.len()).sum();
        let iter = self.chunks.iter().flat_map(|dc| dc.data.iter());

        ChunkedDataIter { iter, size }
    }

    /// Returns an iterator of owned items. This consumes the [`ChunkedData`].
    ///
    /// Note this is currently not just `into_iter` due to how it's implemented, this is subject to change.
    pub fn to_owned_iter(self) -> ChunkedDataIter<impl DoubleEndedIterator<Item = D>> {
        let size = self.chunks.iter().map(|dc| dc.data.len()).sum();
        let iter = self.chunks.into_iter().flat_map(|dc| dc.data.into_iter());

        ChunkedDataIter { iter, size }
    }

    /// Given a slice that serves as the "base" yielding items `T`, return an iterator of `(T, D)`, where each `D` from
    /// the [`ChunkedData`] has its index associated with that of `base_slice`.
    ///
    /// This is meant to be used alongside a slice of time values.
    ///
    /// Note this will return [`None`] if the base slice's length is smaller than that of the [`ChunkedData`].
    pub fn iter_along_base<'a, T>(
        &'a self, base_slice: &'a [T],
    ) -> Option<ChunkedDataIter<impl DoubleEndedIterator<Item = (&'a T, &'a D)>>> {
        if base_slice.len() < self.length() {
            return None;
        }

        let size = self.chunks.iter().map(|dc| dc.data.len()).sum();
        let iter = self.chunks.iter().flat_map(move |dc| {
            let start = dc.start_offset;

            dc.data.iter().enumerate().map(move |(offset, datum)| {
                let actual_index = start + offset;
                let base = &base_slice[actual_index];

                (base, datum)
            })
        });

        Some(ChunkedDataIter { iter, size })
    }

    /// Return how many elements actually are stored in the [`ChunkedData`].
    pub fn num_elements(&self) -> usize {
        self.chunks.iter().map(|dc| dc.data.len()).sum()
    }

    /// Return the "length" of the [`ChunkedData`], _including_ skipped
    /// elements.
    pub fn length(&self) -> usize {
        self.next_index
    }

    /// Push an element.
    pub fn push(&mut self, item: D) {
        if self.is_active {
            let current_chunk = self
                .chunks
                .last_mut()
                .expect("chunks must be initialized with at least a value if is_active is set");
            current_chunk.push(item);
        } else {
            // Start a new chunk.
            self.chunks.push(DataChunk {
                start_offset: self.next_index,
                data: vec![item],
            });
            self.is_active = true;
        }

        self.next_index += 1;
    }

    /// Manually mark that a break is needed in the chunk.
    pub fn insert_break(&mut self) {
        // "Seal" the latest chunk.
        self.is_active = false;
    }

    /// Push an element. If `item` is [`None`], then it will automatically
    /// insert a break in the chunk if needed.
    pub fn try_push(&mut self, item: Option<D>) {
        match item {
            Some(item) => {
                self.push(item);
            }
            None => {
                self.insert_break();
                self.next_index += 1;
            }
        }
    }

    /// Remove all elements up to (and including) `index`, including "skipped"
    /// elements. This will result in the effective length becoming
    /// `prev_length - index - 1`.
    ///
    /// If `index` goes past the number of elements, this function will return
    /// an error containing the stored index in the [`ChunkedData`].
    pub fn prune(&mut self, index: usize) -> Result<(), usize> {
        if self.next_index == 0 || self.next_index - 1 < index || self.chunks.is_empty() {
            return Err(self.next_index);
        }

        self.next_index -= index + 1;

        let dc_index = match self.chunks.binary_search_by(|c| c.start_offset.cmp(&index)) {
            Ok(result) => result,
            Err(result) => {
                if result > 0 {
                    result - 1
                } else {
                    // Nothing to prune. We still need to change the offsets though.
                    for chunk in &mut self.chunks {
                        chunk.start_offset -= index + 1;
                    }

                    return Ok(());
                }
            }
        };

        // SAFETY: This index must be valid since it was returned from the binary search.
        let curr = unsafe { self.chunks.get_unchecked_mut(dc_index) };
        let to_remove = index - curr.start_offset + 1;

        if to_remove <= curr.data.len() {
            curr.data.drain(..to_remove);
            curr.start_offset = 0;

            // Remove all previous chunks.
            self.chunks.drain(0..dc_index);

            // Update offsets for all following chunks.
            for chunk in self.chunks.iter_mut().skip(1) {
                chunk.start_offset -= to_remove;
            }
        } else {
            // Drain this chunk too.
            self.chunks.drain(0..=dc_index);

            for chunk in &mut self.chunks {
                chunk.start_offset -= to_remove;
            }
        }

        Ok(())
    }

    /// Shrink the [`ChunkedData`] after.
    pub fn shrink_to_fit(&mut self) {
        for chunk in &mut self.chunks {
            chunk.data.shrink_to_fit();
        }

        self.chunks.shrink_to_fit();
    }

    /// Convenience function to prune _and_ shrink the [`ChunkedData`] after.
    pub fn prune_and_shrink_to_fit(&mut self, index: usize) -> Result<(), usize> {
        self.prune(index)?;
        self.shrink_to_fit();

        Ok(())
    }

    /// Try and return the first element.
    pub fn first(&self) -> Option<&D> {
        self.chunks.first().and_then(|chunk| chunk.data.first())
    }

    /// Try and return the last element.
    pub fn last(&self) -> Option<&D> {
        self.chunks.last().and_then(|chunk| chunk.data.last())
    }

    /// Return whether there are zero elements left stored internally.
    pub fn no_elements(&self) -> bool {
        self.num_elements() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunked_push() {
        let mut data = ChunkedData::default();
        assert!(data.no_elements());

        data.try_push(Some(1));

        assert!(!data.chunks.is_empty());
        assert!(data.is_active);
        assert_eq!(data.chunks.last().as_ref().unwrap().data, vec![1]);
        assert_eq!(data.next_index, 1);

        data.try_push(Some(2));
        data.try_push(None);

        assert!(!data.is_active);
        assert_eq!(data.chunks.len(), 1);
        assert_eq!(data.chunks.first().unwrap().data, vec![1, 2]);
        assert_eq!(data.next_index, 3);

        data.try_push(None);
        assert!(!data.is_active);
        assert_eq!(data.next_index, 4);

        data.try_push(Some(3));
        assert!(data.is_active);
        assert_eq!(data.chunks.last().as_ref().unwrap().data, vec![3]);
        assert_eq!(data.next_index, 5);

        assert_eq!(data.length(), 5);
        assert_eq!(data.num_elements(), 3);
    }

    /// Ensure that if we push nothing at first, we don't incorrectly try and
    /// seal nothing.
    #[test]
    fn chunked_empty_initial_push() {
        let mut data: ChunkedData<u64> = ChunkedData::default();

        data.try_push(None);
        assert!(!data.is_active);

        data.try_push(Some(1));
        assert!(data.is_active);
        assert_eq!(data.next_index, 2);
    }

    const POPULATION: [Option<u64>; 10] = [
        Some(1),
        Some(2),
        Some(3),
        None,
        None,
        None,
        Some(7),
        Some(8),
        Some(9),
        Some(10),
    ];

    fn test_populate(data: &mut ChunkedData<u64>) {
        for p in POPULATION {
            data.try_push(p);
        }
    }

    /// Initialize data, prune, and insert.
    #[track_caller]
    fn test_pruning(to_prune_index: usize) {
        // println!("Trying to prune up to index {to_prune_index}...");

        let mut data = ChunkedData::default();
        test_populate(&mut data);

        assert!(data.prune(to_prune_index).is_ok());

        let removed = to_prune_index + 1;
        let result = data
            .iter_with_index()
            .map(|(a, b)| (a, *b))
            .collect::<Vec<_>>();

        let expected = POPULATION
            .into_iter()
            .skip(removed)
            .enumerate()
            .filter_map(|(a, b)| b.map(|b| (a, b)))
            .collect::<Vec<_>>();

        assert_eq!(result, expected);
        assert_eq!(data.next_index, POPULATION.len() - (to_prune_index + 1));
    }

    #[test]
    fn chunked_prune() {
        test_pruning(0);
        test_pruning(1);
        test_pruning(2);
        test_pruning(3);
        test_pruning(4);
        test_pruning(5);
        test_pruning(6);
        test_pruning(7);
        test_pruning(8);
        test_pruning(9);
    }

    /// Handle if we try and prune something empty.
    #[test]
    fn chunked_prune_empty() {
        let mut data: ChunkedData<u64> = ChunkedData::default();
        assert!(data.prune(0).is_err());
    }

    /// Handle if we try to clear an index past the index stored.
    #[test]
    fn chunked_prune_past_index() {
        let mut data = ChunkedData::default();
        test_populate(&mut data);

        assert!(data.prune(10).is_err());
    }

    #[test]
    fn chunked_prune_without_curr() {
        let mut data = ChunkedData::default();
        test_populate(&mut data);
        data.try_push(None);

        assert!(data.prune(10).is_ok());
    }

    #[test]
    fn prune_zero_when_none() {
        let mut data = ChunkedData::default();
        data.try_push(None);
        data.try_push(None);
        data.try_push(None);
        test_populate(&mut data);

        assert!(data.prune(0).is_ok());
        assert_eq!(data.chunks[0].start_offset, 2);
        assert_eq!(data.chunks[1].start_offset, 8);
        assert_eq!(data.next_index, POPULATION.len() + 3 - 1);

        assert!(data.prune(0).is_ok());
        assert_eq!(data.chunks[0].start_offset, 1);
        assert_eq!(data.chunks[1].start_offset, 7);
        assert_eq!(data.next_index, POPULATION.len() + 3 - 2);

        assert!(data.prune(0).is_ok());
        assert_eq!(data.chunks[0].start_offset, 0);
        assert_eq!(data.chunks[1].start_offset, 6);
        assert_eq!(data.next_index, POPULATION.len() + 3 - 3);

        assert!(data.prune(0).is_ok());
        assert_eq!(data.chunks[0].start_offset, 0);
        assert_eq!(data.chunks[0].data.as_slice(), &[2, 3]);
        assert_eq!(data.chunks[1].start_offset, 5);
        assert_eq!(data.next_index, POPULATION.len() + 3 - 4);
    }

    #[test]
    fn first_last() {
        let mut data = ChunkedData::default();
        test_populate(&mut data);

        assert_eq!(data.first(), Some(&1));
        assert_eq!(data.last(), Some(&10));
    }

    #[test]
    fn iter() {
        let mut data = ChunkedData::default();
        test_populate(&mut data);

        assert_eq!(
            data.into_iter().collect::<Vec<_>>(),
            POPULATION.iter().filter_map(|v| *v).collect::<Vec<_>>(),
        );
    }

    #[test]
    fn reverse_iter() {
        let mut data = ChunkedData::default();
        test_populate(&mut data);

        assert_eq!(
            data.into_iter().rev().collect::<Vec<_>>(),
            POPULATION
                .iter()
                .filter_map(|v| *v)
                .rev()
                .collect::<Vec<_>>(),
        );
    }
}
