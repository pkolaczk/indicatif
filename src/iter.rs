use crate::progress::ProgressBar;
use std::io;

/// Wraps an iterator to display its progress.
pub trait ProgressIterator
where
    Self: Sized + Iterator,
{
    /// Wrap an iterator with default styling. Attempt to guess iterator
    /// length using `Iterator::size_hint`.
    fn progress(self) -> ProgressBarIter<Self> {
        let n = match self.size_hint() {
            (_, Some(n)) => n as u64,
            _ => 0,
        };
        self.progress_count(n)
    }

    /// Wrap an iterator with an explicit element count.
    fn progress_count(self, len: u64) -> ProgressBarIter<Self> {
        self.progress_with(ProgressBar::new(len))
    }

    /// Wrap an iterator with a custom progress bar.
    fn progress_with(self, progress: ProgressBar) -> ProgressBarIter<Self>;
}

/// Wraps an `Iterator`, `io::Read`, `io::Write`, or `io::Seek` to display its progress.
/// To use it, call one of `ProgressBar::wrap_{iter,read,write}`
#[derive(Debug)]
pub struct ProgressBarIter<T> {
    inner: T,
    progress: ProgressBar,
}

impl<T> ProgressBarIter<T> {
    /// Prefer to use `ProgressBar::wrap_{iter,read,write}` if possible
    pub fn new(inner: T, progress: ProgressBar) -> Self {
        Self { inner, progress }
    }

    pub fn into_inner(self) -> (T, ProgressBar) {
        (self.inner, self.progress)
    }
}

impl<S, T: Iterator<Item = S>> Iterator for ProgressBarIter<T> {
    type Item = S;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next();

        if next.is_some() {
            self.progress.inc(1);
        } else {
            self.progress.finish();
        }

        next
    }
}

impl<S, T: Iterator<Item = S>> ProgressIterator for T {
    fn progress_with(self, progress: ProgressBar) -> ProgressBarIter<Self> {
        ProgressBarIter::new(self, progress)
    }
}

impl<R: io::Read> io::Read for ProgressBarIter<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let inc = self.inner.read(buf)?;
        self.progress.inc(inc as u64);
        Ok(inc)
    }

    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        let inc = self.inner.read_vectored(bufs)?;
        self.progress.inc(inc as u64);
        Ok(inc)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let inc = self.inner.read_to_end(buf)?;
        self.progress.inc(inc as u64);
        Ok(inc)
    }

    fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        let inc = self.inner.read_to_string(buf)?;
        self.progress.inc(inc as u64);
        Ok(inc)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.inner.read_exact(buf)?;
        self.progress.inc(buf.len() as u64);
        Ok(())
    }
}

impl<W: io::Write> io::Write for ProgressBarIter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf).map(|inc| {
            self.progress.inc(inc as u64);
            inc
        })
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice]) -> io::Result<usize> {
        self.inner.write_vectored(bufs).map(|inc| {
            self.progress.inc(inc as u64);
            inc
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.inner.write_all(buf).map(|()| {
            self.progress.inc(buf.len() as u64);
        })
    }

    // write_fmt can not be captured with reasonable effort.
    // as it uses write_all internally by default that should not be a problem.
    // fn write_fmt(&mut self, fmt: fmt::Arguments) -> io::Result<()>;
}

impl<S: io::Seek> io::Seek for ProgressBarIter<S> {
    fn seek(&mut self, f: io::SeekFrom) -> io::Result<u64> {
        let pos = self.inner.seek(f)?;
        self.progress.set_position(pos);
        Ok(pos)
    }
}

#[cfg(test)]
mod test {
    use crate::iter::{ProgressBarIter, ProgressIterator};
    use crate::progress::ProgressBar;

    #[test]
    fn it_can_wrap_an_iterator() {
        let v = vec![1, 2, 3];
        let wrap = |it: ProgressBarIter<_>| {
            assert_eq!(it.map(|x| x * 2).collect::<Vec<_>>(), vec![2, 4, 6]);
        };

        wrap(v.iter().progress());
        wrap(v.iter().progress_count(3));
        wrap({
            let pb = ProgressBar::new(v.len() as u64);
            v.iter().progress_with(pb)
        });
    }
}
