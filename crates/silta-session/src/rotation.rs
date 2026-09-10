//! The rotation state machine (`docs/session-rotation-design.md`), pure: it is fed the
//! events of the stream and the clock, and answers with what the supervisor should do.
//!
//! A rotation is pending once the marker exists (written by the pre-compaction hook
//! or by the supervisor's own threshold). It proceeds at the first quiet moment, no
//! turn in progress and no background agent outstanding, with a handoff request, and
//! ends with the handoff turn's `result`. Two caps bound the two waits; the first
//! expiry cuts the turn and resumes once for the handoff, the second gives up on it.
//! A handoff turn that fails is retried after a pause, at most `HANDOFF_ATTEMPTS`
//! times in all; a failure because the prompt exceeds the window is final at once,
//! since no pause shrinks it. That signal is consulted only for the handoff turn's own
//! failure and never starts or changes a rotation on its own.

use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use crate::stream::Event;

/// The knobs, from the unit's environment.
#[derive(Debug, Clone)]
pub struct Limits {
    pub enabled: bool,
    /// Context size at or above which an idle session is rotated.
    pub context_tokens: u64,
    /// The idle gap, from the last turn end, that the threshold trigger requires.
    pub idle: Duration,
    /// Cap on the wait for a quiet moment after the trigger.
    pub quiet: Duration,
    /// Cap on the handoff turn.
    pub handoff: Duration,
    /// Pause before the next attempt after a handoff turn that failed.
    pub retry_pause: Duration,
}

/// Handoff turns attempted before the rotation gives up on the note.
pub const HANDOFF_ATTEMPTS: u32 = 2;

/// What the supervisor should do now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// The threshold tripped: write the marker.
    MarkPending,
    /// Send the handoff request line.
    RequestHandoff,
    /// The handoff turn ended normally: stop claude and start a fresh session.
    Rotate,
    /// A cap expired with the retry unused: cut the turn, resume once with the combined
    /// line, and wait for the handoff again.
    Retry,
    /// The handoff turn ended with an error: stop, resume normally, and try again after
    /// the pause; `failures` counts the failed handoff turns so far.
    Postpone { failures: u32 },
    /// Stop claude and start a fresh session without a finished handoff.
    GiveUp { reason: Reason },
}

/// Why a rotation gives the handoff up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// A cap expired again after the one cut-and-resume.
    CutTwice,
    /// The handoff turn failed because the prompt exceeds the model's window.
    PromptTooLong,
    /// `HANDOFF_ATTEMPTS` handoff turns failed.
    Failures,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Idle,
    Waiting { deadline: Instant },
    Handoff { deadline: Instant },
}

#[derive(Debug)]
pub struct Tracker {
    limits: Limits,
    turn: bool,
    agents: BTreeSet<String>,
    context: u64,
    last_result: Instant,
    pending: bool,
    not_before: Option<Instant>,
    retry_used: bool,
    failures: u32,
    /// The prompt-too-long signal was seen in the turn in progress.
    too_long: bool,
    phase: Phase,
}

impl Tracker {
    /// A session that just started: its start line is in flight, so a turn is in
    /// progress until its `result`.
    pub fn new(limits: Limits, now: Instant) -> Self {
        Self {
            limits,
            turn: true,
            agents: BTreeSet::new(),
            context: 0,
            last_result: now,
            pending: false,
            not_before: None,
            retry_used: false,
            failures: 0,
            too_long: false,
            phase: Phase::Idle,
        }
    }

    /// The marker existed at the start: the rotation is pending from the first quiet
    /// moment, once `not_before` (the pause after a failed handoff turn) has passed;
    /// `failures` is the count of failed handoff turns so far.
    pub fn pending(mut self, now: Instant, not_before: Option<Instant>, failures: u32) -> Self {
        if !self.limits.enabled {
            return self;
        }
        self.pending = true;
        self.not_before = not_before;
        self.failures = failures;
        let from = not_before.map_or(now, |t| t.max(now));
        self.phase = Phase::Waiting { deadline: from + self.limits.quiet };
        self
    }

    /// The session was resumed with the combined cut-and-handoff line: the retry is
    /// spent and the handoff turn is in flight.
    pub fn retrying(mut self, now: Instant) -> Self {
        self.pending = true;
        self.retry_used = true;
        self.phase = Phase::Handoff { deadline: now + self.limits.handoff };
        self
    }

    pub fn is_pending(&self) -> bool {
        self.pending
    }

    pub fn context(&self) -> u64 {
        self.context
    }

    pub fn quiet(&self) -> bool {
        !self.turn && self.agents.is_empty()
    }

    /// The background agents without a completion, for the journal.
    pub fn agents(&self) -> &BTreeSet<String> {
        &self.agents
    }

    /// The marker appeared on disk.
    pub fn marker_seen(&mut self, now: Instant) -> Vec<Action> {
        if !self.limits.enabled {
            return Vec::new();
        }
        self.set_pending(now);
        self.advance(now)
    }

    pub fn event(&mut self, event: &Event, now: Instant) -> Vec<Action> {
        match event {
            Event::Assistant { context_tokens, prompt_too_long } => {
                self.turn = true;
                self.too_long |= *prompt_too_long;
                if let Some(n) = context_tokens {
                    self.context = *n;
                }
            }
            Event::User => self.turn = true,
            Event::Result { is_error, prompt_too_long } => {
                self.turn = false;
                self.last_result = now;
                let too_long = self.too_long || *prompt_too_long;
                self.too_long = false;
                if let Phase::Handoff { .. } = self.phase {
                    self.phase = Phase::Idle;
                    return if !*is_error {
                        vec![Action::Rotate]
                    } else if too_long {
                        vec![Action::GiveUp { reason: Reason::PromptTooLong }]
                    } else {
                        self.failures += 1;
                        if self.failures >= HANDOFF_ATTEMPTS {
                            vec![Action::GiveUp { reason: Reason::Failures }]
                        } else {
                            self.not_before = Some(now + self.limits.retry_pause);
                            vec![Action::Postpone { failures: self.failures }]
                        }
                    };
                }
            }
            Event::TaskStarted { id } => {
                self.agents.insert(id.clone());
            }
            Event::TaskDone { id } => {
                self.agents.remove(id);
            }
            Event::Init { .. } | Event::Other => {}
        }
        self.advance(now)
    }

    pub fn tick(&mut self, now: Instant) -> Vec<Action> {
        let mut actions = Vec::new();
        if self.limits.enabled
            && !self.pending
            && self.phase == Phase::Idle
            && !self.turn
            && self.context >= self.limits.context_tokens
            && now.duration_since(self.last_result) >= self.limits.idle
        {
            self.set_pending(now);
            actions.push(Action::MarkPending);
        }
        actions.extend(self.advance(now));
        actions
    }

    fn set_pending(&mut self, now: Instant) {
        if !self.pending {
            self.pending = true;
            self.phase = Phase::Waiting { deadline: now + self.limits.quiet };
        }
    }

    fn advance(&mut self, now: Instant) -> Vec<Action> {
        match self.phase {
            Phase::Idle => Vec::new(),
            Phase::Waiting { deadline } => {
                if self.not_before.is_some_and(|t| now < t) {
                    Vec::new()
                } else if self.quiet() {
                    self.phase = Phase::Handoff { deadline: now + self.limits.handoff };
                    vec![Action::RequestHandoff]
                } else if now >= deadline {
                    self.cut()
                } else {
                    Vec::new()
                }
            }
            Phase::Handoff { deadline } => {
                if now >= deadline {
                    self.cut()
                } else {
                    Vec::new()
                }
            }
        }
    }

    fn cut(&mut self) -> Vec<Action> {
        self.phase = Phase::Idle;
        if self.retry_used {
            vec![Action::GiveUp { reason: Reason::CutTwice }]
        } else {
            self.retry_used = true;
            vec![Action::Retry]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Limits {
        Limits {
            enabled: true,
            context_tokens: 300_000,
            idle: Duration::from_secs(4 * 3600),
            quiet: Duration::from_secs(1800),
            handoff: Duration::from_secs(900),
            retry_pause: Duration::from_secs(900),
        }
    }

    fn assistant(context: u64) -> Event {
        Event::Assistant { context_tokens: Some(context), prompt_too_long: false }
    }

    const OK: Event = Event::Result { is_error: false, prompt_too_long: false };
    const ERR: Event = Event::Result { is_error: true, prompt_too_long: false };
    const TOO_LONG: Event = Event::Assistant { context_tokens: Some(0), prompt_too_long: true };
    const ERR_TOO_LONG: Event = Event::Result { is_error: true, prompt_too_long: true };

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn threshold_needs_both_a_large_context_and_the_idle_gap() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0);
        assert!(tr.event(&assistant(310_000), t0 + secs(1)).is_empty());
        assert!(tr.event(&OK, t0 + secs(2)).is_empty());
        // Big context, not idle for long enough.
        assert!(tr.tick(t0 + secs(3600)).is_empty());
        // Idle long enough: the marker, and the handoff at once since it is quiet.
        assert_eq!(
            tr.tick(t0 + secs(2 + 4 * 3600)),
            vec![Action::MarkPending, Action::RequestHandoff]
        );
        // The handoff turn runs and ends: rotate with the handoff done.
        let t = t0 + secs(3 + 4 * 3600);
        assert!(tr.event(&assistant(310_100), t).is_empty());
        assert_eq!(tr.event(&OK, t + secs(60)), vec![Action::Rotate]);
    }

    #[test]
    fn a_small_context_never_trips_and_a_running_turn_is_never_idle() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0);
        tr.event(&assistant(100_000), t0);
        tr.event(&OK, t0);
        assert!(tr.tick(t0 + secs(10 * 3600)).is_empty());
        // A turn that started 5 hours ago without a result: not idle.
        tr.event(&assistant(400_000), t0 + secs(3600));
        assert!(tr.tick(t0 + secs(10 * 3600)).is_empty());
        assert!(!tr.is_pending());
    }

    #[test]
    fn marker_mid_turn_waits_for_the_result_and_for_agents() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0);
        tr.event(&OK, t0);
        tr.event(&assistant(50_000), t0 + secs(1));
        tr.event(&Event::TaskStarted { id: "a1".into() }, t0 + secs(2));
        assert!(tr.marker_seen(t0 + secs(3)).is_empty());
        assert!(tr.is_pending());
        // Turn ends, the agent still runs: keep waiting.
        assert!(tr.event(&OK, t0 + secs(10)).is_empty());
        assert_eq!(tr.agents().len(), 1);
        // The agent reports: quiet.
        assert_eq!(tr.event(&Event::TaskDone { id: "a1".into() }, t0 + secs(20)), vec![Action::RequestHandoff]);
        // A second marker sighting changes nothing.
        assert!(tr.marker_seen(t0 + secs(21)).is_empty());
    }

    #[test]
    fn the_quiet_cap_cuts_once_then_gives_up() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0);
        tr.event(&OK, t0);
        tr.event(&assistant(50_000), t0 + secs(1));
        tr.marker_seen(t0 + secs(2));
        assert!(tr.tick(t0 + secs(2) + secs(1799)).is_empty());
        assert_eq!(tr.tick(t0 + secs(2) + secs(1800)), vec![Action::Retry]);
        // The resumed run: the combined line is the handoff request.
        let t1 = t0 + secs(2000);
        let mut tr = Tracker::new(limits(), t1).retrying(t1);
        tr.event(&assistant(50_000), t1 + secs(1));
        assert!(tr.tick(t1 + secs(899)).is_empty());
        assert_eq!(tr.tick(t1 + secs(900)), vec![Action::GiveUp { reason: Reason::CutTwice }]);
    }

    #[test]
    fn the_retry_resume_can_end_normally() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0).retrying(t0);
        tr.event(&assistant(50_000), t0 + secs(1));
        assert_eq!(tr.event(&OK, t0 + secs(120)), vec![Action::Rotate]);
    }

    #[test]
    fn the_handoff_cap_and_a_failed_handoff_turn() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0);
        assert_eq!(tr.event(&OK, t0), vec![]);
        assert_eq!(tr.marker_seen(t0 + secs(1)), vec![Action::RequestHandoff]);
        tr.event(&assistant(50_000), t0 + secs(2));
        assert_eq!(tr.tick(t0 + secs(1) + secs(900)), vec![Action::Retry]);

        let mut tr = Tracker::new(limits(), t0);
        tr.event(&OK, t0);
        assert_eq!(tr.marker_seen(t0 + secs(1)), vec![Action::RequestHandoff]);
        assert_eq!(tr.event(&ERR, t0 + secs(30)), vec![Action::Postpone { failures: 1 }]);
    }

    #[test]
    fn the_second_failed_handoff_turn_gives_up() {
        let t0 = Instant::now();
        // The resumed run after one failure carries the count.
        let mut tr = Tracker::new(limits(), t0).pending(t0, Some(t0 + secs(900)), 1);
        tr.event(&OK, t0 + secs(5));
        assert_eq!(tr.tick(t0 + secs(900)), vec![Action::RequestHandoff]);
        tr.event(&assistant(50_000), t0 + secs(901));
        assert_eq!(tr.event(&ERR, t0 + secs(930)), vec![Action::GiveUp { reason: Reason::Failures }]);
    }

    #[test]
    fn a_prompt_too_long_failure_of_the_handoff_turn_is_final_at_once() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0);
        tr.event(&OK, t0);
        assert_eq!(tr.marker_seen(t0 + secs(1)), vec![Action::RequestHandoff]);
        assert!(tr.event(&TOO_LONG, t0 + secs(2)).is_empty());
        assert_eq!(tr.event(&ERR, t0 + secs(3)), vec![Action::GiveUp { reason: Reason::PromptTooLong }]);
        // The same from the result's text alone.
        let mut tr = Tracker::new(limits(), t0);
        tr.event(&OK, t0);
        tr.marker_seen(t0 + secs(1));
        assert_eq!(tr.event(&ERR_TOO_LONG, t0 + secs(3)), vec![Action::GiveUp { reason: Reason::PromptTooLong }]);
    }

    #[test]
    fn prompt_too_long_outside_the_handoff_turn_changes_nothing() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0);
        // In the start turn, with no rotation pending: nothing.
        assert!(tr.event(&TOO_LONG, t0 + secs(1)).is_empty());
        assert!(tr.event(&ERR_TOO_LONG, t0 + secs(2)).is_empty());
        assert!(!tr.is_pending());
        assert!(tr.tick(t0 + secs(10)).is_empty());
        // While waiting for quiet: the turn's error is a turn end, the handoff is still
        // requested, and the earlier signal does not carry into it.
        tr.event(&assistant(50_000), t0 + secs(20));
        tr.marker_seen(t0 + secs(21));
        tr.event(&TOO_LONG, t0 + secs(22));
        assert_eq!(tr.event(&ERR_TOO_LONG, t0 + secs(23)), vec![Action::RequestHandoff]);
        tr.event(&assistant(50_000), t0 + secs(24));
        assert_eq!(tr.event(&ERR, t0 + secs(25)), vec![Action::Postpone { failures: 1 }]);
    }

    #[test]
    fn a_pending_start_honours_the_pause_then_rotates() {
        let t0 = Instant::now();
        let pause_end = t0 + secs(900);
        let mut tr = Tracker::new(limits(), t0).pending(t0, Some(pause_end), 0);
        // The start turn ends, but the pause is on.
        assert!(tr.event(&OK, t0 + secs(5)).is_empty());
        assert!(tr.tick(t0 + secs(899)).is_empty());
        assert_eq!(tr.tick(pause_end), vec![Action::RequestHandoff]);
        // The quiet cap counted from the end of the pause, not from the start.
        let mut tr = Tracker::new(limits(), t0).pending(t0, Some(pause_end), 0);
        tr.event(&assistant(1), t0 + secs(5));
        assert!(tr.tick(pause_end + secs(1799)).is_empty());
        assert_eq!(tr.tick(pause_end + secs(1800)), vec![Action::Retry]);
    }

    #[test]
    fn disabled_ignores_markers_and_thresholds() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(Limits { enabled: false, ..limits() }, t0).pending(t0, None, 0);
        tr.event(&assistant(900_000), t0);
        tr.event(&OK, t0);
        assert!(tr.marker_seen(t0 + secs(1)).is_empty());
        assert!(tr.tick(t0 + secs(10 * 3600)).is_empty());
        assert!(!tr.is_pending());
    }
}
