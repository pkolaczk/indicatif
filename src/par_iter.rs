use crate::ProgressBar;
use rayon::iter::{
    plumbing::Consumer, plumbing::Folder, plumbing::UnindexedConsumer, ParallelIterator,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Wraps a Rayon parallel iterator.
///
/// See [`ProgressIterator`](trait.ProgressIterator.html) for method
/// documentation.
pub trait ParallelProgressIterator
where
    Self: Sized + ParallelIterator,
{
    fn progress_with(self, progress: ProgressBar) -> ParProgressBarIter<Self>;

    fn progress_count(self, len: u64) -> ParProgressBarIter<Self> {
        self.progress_with(ProgressBar::new(len))
    }

    fn progress(self) -> ParProgressBarIter<Self> {
        self.progress_count(0)
    }
}

#[derive(Clone, Debug)]
struct IterData(ProgressBar, Arc<(/*update_delta*/ AtomicU64, /*update_counter*/ AtomicU64)>);
impl IterData {
    fn new(pb: ProgressBar) -> Self {
        Self(pb, Arc::new((AtomicU64::new(0), AtomicU64::new(0))))
    }

    fn set_update_delta(&mut self, delta: u64) {
        (self.1).0.store(delta, Ordering::SeqCst);
    }

    fn update(&self, inc: u64) {
        let (delta, counter) = &*self.1;
        let delta = delta.load(Ordering::Acquire);
        let update = loop {
            let mut update = 0;
            let old_val = counter.load(Ordering::Acquire);
            let mut val = old_val;
            val += inc;
            if val > delta {
                update = val / delta;
                val %= delta;
            }
            if counter.compare_and_swap(old_val, val, Ordering::AcqRel) == old_val {
                break update;
            }
        };
        if update > 0 {
            self.0.inc(delta * update);
        }
    }
}

pub struct ParProgressBarIter<T> {
    it: T,
    data: IterData,
}

impl<T> ParProgressBarIter<T> {
    pub(crate) fn new(it: T, progress: ProgressBar) -> Self {
        Self { it, data: IterData::new(progress) }
    }

    pub fn set_update_delta(&mut self, delta: u64) {
        self.data.set_update_delta(delta);
    }
}

impl<S: Send, T: ParallelIterator<Item = S>> ParallelProgressIterator for T {
    fn progress_with(self, progress: ProgressBar) -> ParProgressBarIter<Self> {
        ParProgressBarIter::new(self, progress)
    }
}

struct ProgressConsumer<C> {
    base: C,
    data: IterData,
}

impl<C> ProgressConsumer<C> {
    fn new(base: C, data: IterData) -> Self {
        ProgressConsumer { base, data }
    }
}

impl<T, C: Consumer<T>> Consumer<T> for ProgressConsumer<C> {
    type Folder = ProgressFolder<C::Folder>;
    type Reducer = C::Reducer;
    type Result = C::Result;

    fn split_at(self, index: usize) -> (Self, Self, Self::Reducer) {
        let (left, right, reducer) = self.base.split_at(index);
        (
            ProgressConsumer::new(left, self.data.clone()),
            ProgressConsumer::new(right, self.data),
            reducer,
        )
    }

    fn into_folder(self) -> Self::Folder {
        ProgressFolder {
            base: self.base.into_folder(),
            data: self.data,
        }
    }

    fn full(&self) -> bool {
        self.base.full()
    }
}

impl<T, C: UnindexedConsumer<T>> UnindexedConsumer<T> for ProgressConsumer<C> {
    fn split_off_left(&self) -> Self {
        ProgressConsumer::new(self.base.split_off_left(), self.data.clone())
    }

    fn to_reducer(&self) -> Self::Reducer {
        self.base.to_reducer()
    }
}

struct ProgressFolder<C> {
    base: C,
    data: IterData,
}

impl<T, C: Folder<T>> Folder<T> for ProgressFolder<C> {
    type Result = C::Result;

    fn consume(self, item: T) -> Self {
        self.data.update(1);
        ProgressFolder {
            base: self.base.consume(item),
            data: self.data,
        }
    }

    fn complete(self) -> C::Result {
        self.base.complete()
    }

    fn full(&self) -> bool {
        self.base.full()
    }
}

impl<S: Send, T: ParallelIterator<Item = S>> ParallelIterator for ParProgressBarIter<T> {
    type Item = S;

    fn drive_unindexed<C: UnindexedConsumer<Self::Item>>(self, consumer: C) -> C::Result {
        let consumer1 = ProgressConsumer::new(consumer, self.data);
        self.it.drive_unindexed(consumer1)
    }
}

#[cfg(test)]
mod test {
    use super::{ParProgressBarIter, ParallelProgressIterator};
    use crate::progress::ProgressBar;
    use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

    #[test]
    fn it_can_wrap_a_parallel_iterator() {
        let v = vec![1, 2, 3];
        let wrap = |it: ParProgressBarIter<_>| {
            assert_eq!(it.map(|x| x * 2).collect::<Vec<_>>(), vec![2, 4, 6]);
        };

        wrap(v.par_iter().progress());
        wrap(v.par_iter().progress_count(3));
        wrap({
            let pb = ProgressBar::new(v.len() as u64);
            v.par_iter().progress_with(pb)
        });
    }
}
