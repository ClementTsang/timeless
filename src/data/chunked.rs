//! This is code responsible for possibly chunked data.

#[derive(Default)]
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

    // fn is_empty(&self) -> bool {
    //     self.data.is_empty()
    // }
}

pub struct ChunkedDataIter<I: Iterator> {
    iter: I,
    size: usize,
}

impl<T, I: Iterator<Item = T>> Iterator for ChunkedDataIter<I> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<T, I: Iterator<Item = T>> ExactSizeIterator for ChunkedDataIter<I> {
    fn len(&self) -> usize {
        self.size
    }
}

/// A struct representing data that may potentially have breaks.
/// If you expect that you may want to store time values but _not_
/// data values, use this to avoid storing blanks.
#[derive(Default)]
pub struct ChunkedData<T> {
    next_index: usize,
    is_active: bool,
    chunks: Vec<DataChunk<T>>,
}

impl<T> ChunkedData<T> {
    pub fn iter_index(&self) -> ChunkedDataIter<impl Iterator<Item = (usize, &T)>> {
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

    pub fn iter(&self) -> ChunkedDataIter<impl Iterator<Item = &T>> {
        let size = self.chunks.iter().map(|dc| dc.data.len()).sum();
        let iter = self.chunks.iter().flat_map(|dc| dc.data.iter());

        ChunkedDataIter { iter, size }
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

    /// Push an element. If `item` is [`None`], then it will automatically
    /// insert a break in the chunk if needed.
    pub fn push(&mut self, item: Option<T>) {
        match item {
            Some(item) => {
                if self.is_active {
                    let current_chunk = self.chunks.last_mut().expect(
                        "chunks must be initialized with at least a value if is_active is set",
                    );
                    current_chunk.push(item);
                } else {
                    // Start a new chunk.
                    self.chunks.push(DataChunk {
                        start_offset: self.next_index,
                        data: vec![item],
                    });
                    self.is_active = true;
                }
            }
            None => {
                // "Seal" the latest chunk.
                self.is_active = false;
            }
        }

        self.next_index += 1;
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

        let dc_index = match self.chunks.binary_search_by(|c| c.start_offset.cmp(&index)) {
            Ok(index) => index,
            Err(index) => index - 1,
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

            for chunk in self.chunks.iter_mut() {
                chunk.start_offset -= to_remove;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunked_push() {
        let mut data = ChunkedData::default();

        data.push(Some(1));

        assert!(!data.chunks.is_empty());
        assert!(data.is_active);
        assert_eq!(data.chunks.last().as_ref().unwrap().data, vec![1]);
        assert_eq!(data.next_index, 1);

        data.push(Some(2));
        data.push(None);

        assert!(!data.is_active);
        assert_eq!(data.chunks.len(), 1);
        assert_eq!(data.chunks.get(0).unwrap().data, vec![1, 2]);
        assert_eq!(data.next_index, 3);

        data.push(None);
        assert!(!data.is_active);
        assert_eq!(data.next_index, 4);

        data.push(Some(3));
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

        data.push(None);
        assert!(!data.is_active);

        data.push(Some(1));
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
            data.push(p);
        }
    }

    /// Initialize data, prune, and insert.
    #[track_caller]
    fn test_pruning(to_prune_index: usize) {
        println!("Trying to prune up to index {to_prune_index}...");

        let mut data = ChunkedData::default();
        test_populate(&mut data);

        assert!(data.prune(to_prune_index).is_ok());

        let removed = to_prune_index + 1;
        let result = data.iter_index().map(|(a, b)| (a, *b)).collect::<Vec<_>>();

        let expected = POPULATION
            .into_iter()
            .skip(removed)
            .enumerate()
            .filter_map(|(a, b)| b.map(|b| (a, b)))
            .collect::<Vec<_>>();

        assert_eq!(result, expected);
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
        data.push(None);
    }
}
