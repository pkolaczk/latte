use futures::stream::{Fuse, Skip};
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::interval;
use tokio::time::{Duration, MissedTickBehavior};
use tokio_stream::wrappers::IntervalStream;

pub trait ChunksExt: Stream {
    /// Splits the stream into chunks delimited by time or by number of items.
    ///
    /// When polled, it collects the items from the original stream into the current chunk
    /// until the desired number of items is collected or until the period of time passes.
    /// Then it emits the chunk and sets a new one as the current one and the cycle repeats.
    /// Can emit an empty chunk if no items from the original stream were ready before the
    /// period of time elapses.
    ///
    /// # Parameters
    /// - `count`: maximum number of items added to a chunk
    /// - `period`: maximum amount of time a chunk can be kept before releasing it
    /// - `new_chunk`: a function to create an empty chunk
    /// - `accumulate`: a function to add original stream items to the current chunk
    fn chunks_aggregated<Chunk, NewChunkFn, AccumulateFn>(
        self,
        count: u64,
        period: Duration,
        new_chunk: NewChunkFn,
        accumulate: AccumulateFn,
    ) -> ChunksAggregated<Self, Chunk, NewChunkFn, AccumulateFn>
    where
        Self: Sized,
        NewChunkFn: Fn() -> Chunk,
        AccumulateFn: Fn(&mut Chunk, Self::Item),
    {
        ChunksAggregated::new(self, count, period, new_chunk, accumulate)
    }
}

impl<S: Stream> ChunksExt for S {}

#[pin_project]
pub struct ChunksAggregated<Src, Chunk, NewChunkFn, AddFn> {
    #[pin]
    src: Fuse<Src>,
    new_chunk: NewChunkFn,
    accumulate: AddFn,
    max_chunk_size: u64,
    #[pin]
    clock: Clock,
    current_chunk: Option<Chunk>,
    current_chunk_size: u64,
}

#[pin_project(project = ClockProj)]
enum Clock {
    Some(#[pin] Skip<IntervalStream>),
    None,
}

impl<Src, Item, Chunk, NewChunkFn, AccumulateFn>
    ChunksAggregated<Src, Chunk, NewChunkFn, AccumulateFn>
where
    Src: Stream<Item = Item>,
    NewChunkFn: Fn() -> Chunk,
    AccumulateFn: Fn(&mut Chunk, Item),
{
    pub fn new(
        src: Src,
        max_chunk_size: u64,
        period: Duration,
        new_chunk: NewChunkFn,
        accumulate: AccumulateFn,
    ) -> Self {
        let clock = if period < Duration::MAX {
            let mut interval = interval(period);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            Clock::Some(IntervalStream::new(interval).skip(1))
        } else {
            Clock::None
        };

        let current_chunk = Some(new_chunk());

        Self {
            new_chunk,
            accumulate,
            src: src.fuse(),
            max_chunk_size,
            clock,
            current_chunk,
            current_chunk_size: 0,
        }
    }

    fn next_batch(self: Pin<&mut Self>) -> Option<Chunk> {
        let this = self.project();
        *this.current_chunk_size = 0;
        this.current_chunk.replace((this.new_chunk)())
    }

    fn final_batch(self: Pin<&mut Self>) -> Option<Chunk> {
        let this = self.project();
        *this.current_chunk_size = 0;
        this.current_chunk.take()
    }
}

impl<Src, Item, Chunk, NewChunkFn, AddFn> Stream for ChunksAggregated<Src, Chunk, NewChunkFn, AddFn>
where
    Item: Debug,
    Src: Stream<Item = Item>,
    NewChunkFn: Fn() -> Chunk,
    AddFn: Fn(&mut Chunk, Item),
{
    type Item = Chunk;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        // Add all ready items in the source stream to the current chunk:
        while this.current_chunk_size < this.max_chunk_size {
            match this.src.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *this.current_chunk_size += 1;
                    let chunk = this.current_chunk.as_mut().expect("chunk must be set");
                    (this.accumulate)(chunk, item);
                }
                Poll::Ready(None) => {
                    return Poll::Ready(self.final_batch());
                }
                Poll::Pending => {
                    // No more items, but we can't leave yet, we need to check the clock
                    // at the end of the loop
                    break;
                }
            }
        }
        let deadline_reached = match this.clock.as_mut().project() {
            ClockProj::Some(clock) => clock.poll_next(cx).is_ready(),
            ClockProj::None => false,
        };

        if deadline_reached || this.current_chunk_size >= this.max_chunk_size {
            Poll::Ready(self.next_batch())
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod test {
    use crate::chunks::{ChunksAggregated, ChunksExt};
    use futures::{stream, FutureExt, StreamExt};
    use std::time::Duration;
    use tokio::time::interval;
    use tokio_stream::wrappers::IntervalStream;

    #[tokio::test]
    async fn test_empty() {
        let s = stream::empty::<u64>();
        let batched = ChunksAggregated::new(s, 2, Duration::from_secs(100), Vec::new, Vec::push);
        let results: Vec<_> = batched.collect().await;
        assert_eq!(results, vec![vec![0; 0]]);
    }

    #[tokio::test]
    async fn test_count() {
        let s = stream::iter(vec![1, 2, 3, 4, 5]);
        let batched = ChunksAggregated::new(s, 2, Duration::from_secs(100), Vec::new, Vec::push);
        let results: Vec<_> = batched.collect().await;
        assert_eq!(results, vec![vec![1, 2], vec![3, 4], vec![5]]);
    }

    #[tokio::test]
    async fn test_period() {
        tokio::time::pause();

        let s = IntervalStream::new(interval(Duration::from_secs(1)))
            .enumerate()
            .map(|x| x.0)
            .skip(1)
            .take(5);
        let mut batched =
            s.chunks_aggregated(u64::MAX, Duration::from_secs(2), Vec::new, Vec::push);
        assert!(batched.next().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(batched.next().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(batched.next().await, Some(vec![1, 2]));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(batched.next().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(batched.next().await, Some(vec![3, 4]));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(batched.next().await, Some(vec![5]));
    }
}
