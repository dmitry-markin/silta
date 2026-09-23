//! Which inbound messages to deliver after a (re)start.
//!
//! The first sync after a start replays recent history. Without a guard every restart
//! would re-deliver old messages; with a plain "older than start" guard a message
//! written during the downtime is lost. The verdict combines a per-room watermark (the
//! last delivered message) with a replay window: anything newer than the watermark and
//! at most `window` before the start is delivered.

use serde::{Deserialize, Serialize};

/// The last message delivered in a room.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watermark {
    pub ts_ms: u64,
    pub event_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Deliver. `behind_start_ms` is how long before the daemon start the message was
    /// sent, 0 for a live message.
    Deliver { behind_start_ms: u64 },
    /// At or before the watermark: delivered by an earlier run.
    AlreadyDelivered,
    /// Older than the replay window.
    TooOld,
}

pub fn verdict(
    ts_ms: u64,
    event_id: &str,
    mark: Option<&Watermark>,
    started_at_ms: u64,
    window_ms: u64,
) -> Verdict {
    if let Some(mark) = mark {
        if ts_ms < mark.ts_ms || (ts_ms == mark.ts_ms && event_id == mark.event_id) {
            return Verdict::AlreadyDelivered;
        }
    }
    if ts_ms + window_ms < started_at_ms {
        return Verdict::TooOld;
    }
    Verdict::Deliver {
        behind_start_ms: started_at_ms.saturating_sub(ts_ms),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const START: u64 = 1_000_000;
    const WINDOW: u64 = 300_000;

    #[test]
    fn fresh_start_uses_the_window() {
        assert_eq!(
            verdict(START + 5, "$a", None, START, WINDOW),
            Verdict::Deliver { behind_start_ms: 0 }
        );
        assert_eq!(
            verdict(START - 40_000, "$a", None, START, WINDOW),
            Verdict::Deliver {
                behind_start_ms: 40_000
            }
        );
        assert_eq!(
            verdict(START - WINDOW, "$a", None, START, WINDOW),
            Verdict::Deliver {
                behind_start_ms: WINDOW
            }
        );
        assert_eq!(
            verdict(START - WINDOW - 1, "$a", None, START, WINDOW),
            Verdict::TooOld
        );
    }

    #[test]
    fn watermark_stops_re_delivery() {
        let mark = Watermark {
            ts_ms: START - 10_000,
            event_id: "$m".into(),
        };
        // The marked message itself and anything older: already delivered.
        assert_eq!(
            verdict(START - 10_000, "$m", Some(&mark), START, WINDOW),
            Verdict::AlreadyDelivered
        );
        assert_eq!(
            verdict(START - 20_000, "$older", Some(&mark), START, WINDOW),
            Verdict::AlreadyDelivered
        );
        // A different message in the same millisecond is new.
        assert_eq!(
            verdict(START - 10_000, "$twin", Some(&mark), START, WINDOW),
            Verdict::Deliver {
                behind_start_ms: 10_000
            }
        );
        // Newer than the mark but within the window: the downtime message.
        assert_eq!(
            verdict(START - 5_000, "$down", Some(&mark), START, WINDOW),
            Verdict::Deliver {
                behind_start_ms: 5_000
            }
        );
        // A window of zero is the old "older than start" rule.
        assert_eq!(verdict(START - 1, "$x", None, START, 0), Verdict::TooOld);
    }
}
