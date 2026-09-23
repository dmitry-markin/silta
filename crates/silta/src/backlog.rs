//! A bounded FIFO of items waiting for a session that is not connected.
//!
//! Bounded twice: by count (the oldest goes first) and by age against the caller's
//! clock, so a session that comes back after a long absence gets recent messages only.

use std::collections::VecDeque;

#[derive(Debug)]
pub struct Backlog<T> {
    items: VecDeque<(u64, T)>,
    max_len: usize,
    max_age_ms: u64,
}

impl<T> Backlog<T> {
    pub fn new(max_len: usize, max_age_ms: u64) -> Self {
        Backlog {
            items: VecDeque::new(),
            max_len,
            max_age_ms,
        }
    }

    /// Queue an item stamped `ts_ms`; returns how many older items were evicted.
    pub fn push(&mut self, ts_ms: u64, item: T, now_ms: u64) -> usize {
        let mut evicted = self.evict_expired(now_ms);
        while self.items.len() >= self.max_len.max(1) {
            self.items.pop_front();
            evicted += 1;
        }
        self.items.push_back((ts_ms, item));
        evicted
    }

    /// Take everything still within the age limit, oldest first; returns the items with
    /// their timestamps and how many expired ones were dropped.
    pub fn drain(&mut self, now_ms: u64) -> (Vec<(u64, T)>, usize) {
        let evicted = self.evict_expired(now_ms);
        (self.items.drain(..).collect(), evicted)
    }

    /// Put items back in front of the queue, in the order given: they were handed to a
    /// connection that went away, so they are older than anything queued since. Returns
    /// how many were evicted for age or count.
    pub fn restore(&mut self, items: Vec<(u64, T)>, now_ms: u64) -> usize {
        for item in items.into_iter().rev() {
            self.items.push_front(item);
        }
        let mut evicted = self.evict_expired(now_ms);
        while self.items.len() > self.max_len.max(1) {
            self.items.pop_front();
            evicted += 1;
        }
        evicted
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn evict_expired(&mut self, now_ms: u64) -> usize {
        let mut evicted = 0;
        while let Some((ts, _)) = self.items.front() {
            if ts.saturating_add(self.max_age_ms) < now_ms {
                self.items.pop_front();
                evicted += 1;
            } else {
                break;
            }
        }
        evicted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo_within_limits() {
        let mut b = Backlog::new(10, 1_000);
        assert_eq!(b.push(100, "a", 100), 0);
        assert_eq!(b.push(200, "b", 200), 0);
        let (items, evicted) = b.drain(300);
        assert_eq!(items, vec![(100, "a"), (200, "b")]);
        assert_eq!(evicted, 0);
        assert!(b.is_empty());
    }

    #[test]
    fn count_cap_drops_the_oldest() {
        let mut b = Backlog::new(2, 1_000_000);
        b.push(1, "a", 1);
        b.push(2, "b", 2);
        assert_eq!(b.push(3, "c", 3), 1);
        assert_eq!(b.len(), 2);
        assert_eq!(b.drain(3).0, vec![(2, "b"), (3, "c")]);
    }

    #[test]
    fn restored_items_go_first() {
        let mut b = Backlog::new(3, 1_000);
        b.push(300, "new", 300);
        assert_eq!(b.restore(vec![(100, "a"), (200, "b")], 300), 0);
        assert_eq!(b.drain(300).0, vec![(100, "a"), (200, "b"), (300, "new")]);
        // The age cap and then the count cap take the oldest restored ones.
        b.push(500, "x", 500);
        assert_eq!(
            b.restore(
                vec![(1, "expired"), (350, "w"), (400, "y"), (450, "z")],
                1_200
            ),
            2
        );
        assert_eq!(b.drain(1_200).0, vec![(400, "y"), (450, "z"), (500, "x")]);
    }

    #[test]
    fn age_cap_drops_expired_on_push_and_drain() {
        let mut b = Backlog::new(10, 1_000);
        b.push(0, "old", 0);
        b.push(500, "mid", 500);
        // At t=1200 the first item (0 + 1000 < 1200) is expired, the second is not.
        assert_eq!(b.push(1_200, "new", 1_200), 1);
        assert_eq!(b.len(), 2);
        // Much later everything has expired.
        let (items, evicted) = b.drain(10_000);
        assert!(items.is_empty());
        assert_eq!(evicted, 2);
    }
}
