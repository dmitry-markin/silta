//! Watches for Claude Code output that no longer matches what the supervisor assumes,
//! so that a schema change after an upgrade shows in the journal as a `contract:` line
//! instead of a silently blind supervisor. Pure: it answers with the line to print,
//! if any.

use std::time::{Duration, Instant};

use crate::stream::Event;

/// The Claude Code versions whose output the supervisor was checked against; any other
/// version gets a line at every start until the list is extended.
pub const TESTED_VERSIONS: &[&str] = &["2.1.281"];

#[derive(Debug, Default)]
pub struct Contract {
    /// When the main line first spoke in the turn in progress.
    spoke_at: Option<Instant>,
    warned_usage: bool,
    warned_turn: bool,
}

impl Contract {
    /// `rotation` is whether rotation is on, in which case the hook should block
    /// automatic compactions.
    pub fn event(&mut self, event: &Event, rotation: bool, now: Instant) -> Option<String> {
        match event {
            Event::Init { version, .. } if !TESTED_VERSIONS.contains(&version.as_str()) => Some(format!(
                "Claude Code {version} was not checked against this supervisor (checked: {})",
                TESTED_VERSIONS.join(", ")
            )),
            Event::Assistant { subagent: false, context_tokens } => {
                self.spoke_at.get_or_insert(now);
                if context_tokens.is_none() && !self.warned_usage {
                    self.warned_usage = true;
                    return Some("an assistant line without usage; the context size is unknown and the threshold rotation is blind".to_owned());
                }
                None
            }
            Event::Result { .. } => {
                self.spoke_at = None;
                self.warned_turn = false;
                None
            }
            Event::Compacted { auto: true } if rotation => {
                Some("Claude Code compacted the conversation on its own; the PreCompact hook did not block it".to_owned())
            }
            _ => None,
        }
    }

    /// A turn longer than `cap` (the quiet cap) is more likely a missing `result` line
    /// than a turn; said once per turn.
    pub fn tick(&mut self, now: Instant, cap: Duration) -> Option<String> {
        let spoke_at = self.spoke_at?;
        if self.warned_turn || now.duration_since(spoke_at) < cap {
            return None;
        }
        self.warned_turn = true;
        Some(format!("no result line for {} s after an assistant line; a very long turn, or the result line has changed", cap.as_secs()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAP: Duration = Duration::from_secs(1800);
    const OK: Event = Event::Result {
        is_error: false,
        person: false,
    };

    fn assistant() -> Event {
        Event::Assistant {
            subagent: false,
            context_tokens: Some(10),
        }
    }

    fn init(version: &str) -> Event {
        Event::Init {
            session_id: "s".into(),
            version: version.into(),
        }
    }

    #[test]
    fn the_version_gate() {
        let mut c = Contract::default();
        let t0 = Instant::now();
        assert!(c.event(&init(TESTED_VERSIONS[0]), true, t0).is_none());
        let line = c.event(&init("9.9.9"), true, t0).unwrap();
        assert!(
            line.starts_with("Claude Code 9.9.9 was not checked"),
            "{line}"
        );
        assert!(c.event(&init("?"), true, t0).is_some());
    }

    #[test]
    fn a_long_turn_is_measured_from_its_first_assistant_line_not_from_the_last_result() {
        let mut c = Contract::default();
        let t0 = Instant::now();
        c.event(&assistant(), true, t0);
        c.event(&OK, true, t0 + Duration::from_secs(5));
        // Two idle hours, then a person's turn starts: no warning at once.
        let t1 = t0 + Duration::from_secs(2 * 3600);
        assert!(c.event(&assistant(), true, t1).is_none());
        assert!(c.tick(t1 + Duration::from_secs(1), CAP).is_none());
        assert!(c.tick(t1 + CAP - Duration::from_secs(1), CAP).is_none());
        // The cap after the turn's first line: once.
        assert!(c.tick(t1 + CAP, CAP).is_some());
        assert!(c.tick(t1 + CAP + Duration::from_secs(60), CAP).is_none());
        // A result ends the turn and re-arms the check.
        c.event(&OK, true, t1 + CAP + Duration::from_secs(61));
        assert!(c.tick(t1 + 2 * CAP, CAP).is_none());
        // A subagent's line does not start a turn.
        c.event(
            &Event::Assistant {
                subagent: true,
                context_tokens: Some(1),
            },
            true,
            t1 + 2 * CAP,
        );
        assert!(c.tick(t1 + 4 * CAP, CAP).is_none());
    }

    #[test]
    fn usage_missing_is_said_once_and_compaction_only_with_rotation_on() {
        let mut c = Contract::default();
        let t0 = Instant::now();
        let no_usage = Event::Assistant {
            subagent: false,
            context_tokens: None,
        };
        assert!(c.event(&no_usage, true, t0).is_some());
        assert!(c.event(&no_usage, true, t0).is_none());
        assert!(c
            .event(
                &Event::Assistant {
                    subagent: true,
                    context_tokens: None
                },
                true,
                t0
            )
            .is_none());
        assert!(c
            .event(&Event::Compacted { auto: true }, false, t0)
            .is_none());
        assert!(c
            .event(&Event::Compacted { auto: false }, true, t0)
            .is_none());
        assert!(c
            .event(&Event::Compacted { auto: true }, true, t0)
            .is_some());
    }
}
