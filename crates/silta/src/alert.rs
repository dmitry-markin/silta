//! The daemon's alerts to the owner: a session away for too long, a session silent
//! after a delivery, and the note when a session is back. Pure bookkeeping; the daemon
//! decides when to call it and how to send what it returns.
//!
//! Two signals, because a Claude Code session whose token is rejected does not exit
//! (checked 2026-09-06): it answers every message with an error result and its plugin
//! stays connected, so "disconnected" alone would miss it. The second signal is the
//! typing refresh reaching its cap without a reply, a reaction, an edit or a file.

use std::collections::HashMap;

/// An away alert is repeated this often while the session stays away.
pub const REPEAT_MS: u64 = 24 * 60 * 60 * 1000;
/// A silent alert is sent at most this often per session.
pub const SILENT_REPEAT_MS: u64 = 60 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Alert {
    /// The session has been disconnected since `since_ms`.
    Away { session: String, since_ms: u64 },
    /// The session connected after an outage the owner was told about.
    Back { session: String, away_ms: u64 },
    /// A message delivered at `delivered_ms` got no visible action within the cap.
    Silent {
        session: String,
        room_id: String,
        delivered_ms: u64,
    },
}

#[derive(Debug, Default)]
struct State {
    disconnected_since: Option<u64>,
    alerted_at: Option<u64>,
    silent_alerted_at: Option<u64>,
}

/// One entry per configured session. Every session starts away, since the daemon's
/// start: a mind that never connects is the first case the alert is for.
#[derive(Debug)]
pub struct Watch {
    grace_ms: u64,
    sessions: HashMap<String, State>,
}

impl Watch {
    /// `grace_secs` of 0 disables every alert.
    pub fn new(sessions: impl IntoIterator<Item = String>, grace_secs: u64, now_ms: u64) -> Watch {
        let sessions = sessions
            .into_iter()
            .map(|name| {
                (
                    name,
                    State {
                        disconnected_since: Some(now_ms),
                        ..Default::default()
                    },
                )
            })
            .collect();
        Watch {
            grace_ms: grace_secs.saturating_mul(1000),
            sessions,
        }
    }

    pub fn enabled(&self) -> bool {
        self.grace_ms > 0
    }

    /// A session completed its handshake. Tells the owner if they were told it was away.
    pub fn connected(&mut self, session: &str, now_ms: u64) -> Option<Alert> {
        let state = self.sessions.get_mut(session)?;
        let since = state.disconnected_since.take();
        let alerted = state.alerted_at.take();
        match (alerted, since) {
            (Some(_), Some(since)) => Some(Alert::Back {
                session: session.to_owned(),
                away_ms: now_ms.saturating_sub(since),
            }),
            _ => None,
        }
    }

    /// A session's connection went away.
    pub fn disconnected(&mut self, session: &str, now_ms: u64) {
        if let Some(state) = self.sessions.get_mut(session) {
            if state.disconnected_since.is_none() {
                state.disconnected_since = Some(now_ms);
            }
        }
    }

    /// The away alerts due now: sessions away longer than the grace period and not
    /// reported in the last [`REPEAT_MS`].
    pub fn tick(&mut self, now_ms: u64) -> Vec<Alert> {
        if !self.enabled() {
            return Vec::new();
        }
        let mut due = Vec::new();
        let mut names: Vec<&String> = self.sessions.keys().collect();
        names.sort();
        let names: Vec<String> = names.into_iter().cloned().collect();
        for name in names {
            let state = self.sessions.get_mut(&name).expect("listed");
            let Some(since) = state.disconnected_since else {
                continue;
            };
            if now_ms.saturating_sub(since) < self.grace_ms {
                continue;
            }
            if state
                .alerted_at
                .is_some_and(|at| now_ms.saturating_sub(at) < REPEAT_MS)
            {
                continue;
            }
            state.alerted_at = Some(now_ms);
            due.push(Alert::Away {
                session: name.clone(),
                since_ms: since,
            });
        }
        due
    }

    /// A delivered message saw no visible action within the cap. At most one alert
    /// per session per [`SILENT_REPEAT_MS`].
    pub fn silent(
        &mut self,
        session: &str,
        room_id: &str,
        delivered_ms: u64,
        now_ms: u64,
    ) -> Option<Alert> {
        if !self.enabled() {
            return None;
        }
        let state = self.sessions.get_mut(session)?;
        if state
            .silent_alerted_at
            .is_some_and(|at| now_ms.saturating_sub(at) < SILENT_REPEAT_MS)
        {
            return None;
        }
        state.silent_alerted_at = Some(now_ms);
        Some(Alert::Silent {
            session: session.to_owned(),
            room_id: room_id.to_owned(),
            delivered_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: u64 = 60_000;

    fn watch() -> Watch {
        Watch::new(["hub".to_owned(), "alice".to_owned()], 600, 0)
    }

    #[test]
    fn a_session_that_never_connects_is_reported_once_the_grace_period_passes() {
        let mut w = watch();
        assert!(w.tick(9 * MIN).is_empty());
        let due = w.tick(10 * MIN);
        assert_eq!(
            due,
            vec![
                Alert::Away {
                    session: "alice".into(),
                    since_ms: 0
                },
                Alert::Away {
                    session: "hub".into(),
                    since_ms: 0
                }
            ]
        );
        // Not again until a day has passed.
        assert!(w.tick(11 * MIN).is_empty());
        assert!(w.tick(10 * MIN + REPEAT_MS - 1).is_empty());
        assert_eq!(w.tick(10 * MIN + REPEAT_MS).len(), 2);
    }

    #[test]
    fn connecting_in_time_reports_nothing_and_a_late_return_is_announced() {
        let mut w = watch();
        assert_eq!(w.connected("alice", 5 * MIN), None);
        w.connected("hub", 5 * MIN);
        assert!(w.tick(60 * MIN).is_empty());

        w.disconnected("alice", 60 * MIN);
        assert!(w.tick(69 * MIN).is_empty());
        assert_eq!(
            w.tick(70 * MIN),
            vec![Alert::Away {
                session: "alice".into(),
                since_ms: 60 * MIN
            }]
        );
        assert_eq!(
            w.connected("alice", 85 * MIN),
            Some(Alert::Back {
                session: "alice".into(),
                away_ms: 25 * MIN
            })
        );
        // Back, and a new outage counts from its own start.
        assert!(w.tick(90 * MIN).is_empty());
        w.disconnected("alice", 90 * MIN);
        w.disconnected("alice", 91 * MIN); // a second call keeps the first time
        assert_eq!(
            w.tick(100 * MIN),
            vec![Alert::Away {
                session: "alice".into(),
                since_ms: 90 * MIN
            }]
        );
    }

    #[test]
    fn silent_alerts_are_rate_limited_per_session() {
        let mut w = watch();
        w.connected("alice", 0);
        let a = w.silent("alice", "!r", 100 * MIN, 110 * MIN);
        assert_eq!(
            a,
            Some(Alert::Silent {
                session: "alice".into(),
                room_id: "!r".into(),
                delivered_ms: 100 * MIN
            })
        );
        assert_eq!(w.silent("alice", "!r", 120 * MIN, 130 * MIN), None);
        assert!(w.silent("hub", "!g", 120 * MIN, 130 * MIN).is_some());
        assert!(w
            .silent("alice", "!r", 170 * MIN, 110 * MIN + SILENT_REPEAT_MS)
            .is_some());
        assert_eq!(w.silent("nobody", "!r", 0, 0), None);
    }

    #[test]
    fn grace_zero_disables_everything() {
        let mut w = Watch::new(["hub".to_owned()], 0, 0);
        assert!(!w.enabled());
        assert!(w.tick(REPEAT_MS * 10).is_empty());
        assert_eq!(w.silent("hub", "!r", 0, MIN * 100), None);
        assert_eq!(w.connected("hub", MIN), None);
    }
}
