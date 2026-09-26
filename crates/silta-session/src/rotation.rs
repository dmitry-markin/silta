//! The rotation state machine, pure: it is fed the events of the stream and the clock,
//! and answers with what the supervisor should do.
//!
//! A rotation is pending once the marker exists (written by the pre-compaction hook
//! or by the supervisor's own threshold).
//!
//! It proceeds at the first quiet moment, no turn in progress and no background task
//! (an agent, a command) outstanding, with a handoff request, and the handoff turn is
//! the turn that wrote the handoff note (a turn a person's message started can end
//! first, and does not count).
//!
//! The session is then compacted in place with `/compact` and keeps its id, timers and
//! agents.
//!
//! Three caps bound the waits (for the quiet moment, the handoff turn and the
//! compaction); a cap that expires cuts the turn and resumes the session for the handoff,
//! and a handoff turn or a compaction that fails is retried after a pause, from the
//! handoff again. One count of attempts covers cuts and failures alike and survives
//! the restarts they cause: `HANDOFF_ATTEMPTS` of them in all end the rotation with
//! a fresh session id, the last resort.
//!
//! The supervisor snapshots memory and transcript at two moments the machine names:
//! before the handoff is first requested, and after each handoff turn.

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
    /// The gap without a message from a person that the threshold trigger requires.
    pub idle: Duration,
    /// Cap on the wait for a quiet moment after the trigger.
    pub quiet: Duration,
    /// Cap on the handoff turn.
    pub handoff: Duration,
    /// Cap on the compaction after the handoff turn, counted from the `/compact` line.
    pub compact: Duration,
    /// Pause before the next attempt after a handoff turn that failed.
    pub retry_pause: Duration,
}

/// Attempts of one rotation (a cut turn, a failed handoff turn or a failed compaction
/// each spend one) before it gives the compaction up for a fresh session id.
pub const HANDOFF_ATTEMPTS: u32 = 2;

/// The two moments at which memory and transcript are copied: both are idle moments,
/// so the files are consistent, and the two copies are the clean state before the
/// handoff attempt and the state a fresh session would start from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Moment {
    BeforeHandoff,
    AfterHandoff,
}

impl Moment {
    pub fn name(self) -> &'static str {
        match self {
            Moment::BeforeHandoff => "before-handoff",
            Moment::AfterHandoff => "after-handoff",
        }
    }
}

/// The step of the rotation that was cut or that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// The wait for a quiet moment: no handoff line has gone out yet.
    Wait,
    Handoff,
    Compaction,
}

impl Step {
    /// What was cut or failed, for the journal.
    pub fn what(self) -> &'static str {
        match self {
            Step::Wait => "the turn",
            Step::Handoff => "the handoff turn",
            Step::Compaction => "the compaction",
        }
    }
}

/// What the supervisor should do now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// The threshold tripped: write the marker.
    MarkPending,
    /// Copy memory and transcript to `backups/`.
    Snapshot(Moment),
    /// Send the handoff request line.
    RequestHandoff,
    /// A turn ended without the handoff note written: keep waiting for the handoff.
    AwaitHandoff,
    /// The handoff turn ended normally: send `/compact`.
    Compact,
    /// A turn ended with neither a compaction boundary nor a failure: keep waiting.
    AwaitCompaction,
    /// The compaction went through: the rotation is complete, the session goes on.
    Compacted,
    /// A cap expired with an attempt left: cut the turn, resume with the combined
    /// line, and wait for the handoff again. `step` is what was cut; after `Wait` the
    /// before-handoff snapshot is still due. `attempts` counts this one, and `handoff`
    /// says whether a handoff turn of the rotation has completed (the note written).
    Retry {
        step: Step,
        attempts: u32,
        handoff: bool,
    },
    /// The handoff turn ended with an error, or the compaction failed, with an attempt
    /// left: stop, resume normally, and try again from the handoff after the pause.
    /// `attempts` and `handoff` as for `Retry`.
    Postpone {
        step: Step,
        attempts: u32,
        handoff: bool,
    },
    /// The attempts are spent: stop claude and start a fresh session. `step` is the
    /// last attempt's and `cut` whether its cap expired (else it failed); `handoff`
    /// says whether a handoff turn of the rotation completed, so that the fresh
    /// session is told whether the note in memory is the rotated session's.
    GiveUp {
        step: Step,
        cut: bool,
        handoff: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Idle,
    Waiting {
        deadline: Instant,
    },
    Handoff {
        deadline: Instant,
    },
    /// `/compact` has been sent; `boundary` once the compaction boundary arrived,
    /// `failed` once Claude Code reported the compaction failed.
    Compacting {
        deadline: Instant,
        boundary: bool,
        failed: bool,
    },
}

#[derive(Debug)]
pub struct Tracker {
    limits: Limits,
    turn: bool,
    agents: BTreeSet<String>,
    context: u64,
    /// The last message from a person (a channel delivery), or the start: the idle gap
    /// is measured from it, so that timer wakeups and the mind's own work never pass
    /// for the person's presence.
    last_person: Instant,
    pending: bool,
    not_before: Option<Instant>,
    /// Cuts and failures of the pending rotation so far.
    attempts: u32,
    /// A handoff turn of the pending rotation has completed: the note is written.
    handoff: bool,
    /// The handoff note has been written since it was requested, as the supervisor
    /// found on disk before the turn's `result`.
    note_written: bool,
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
            last_person: now,
            pending: false,
            not_before: None,
            attempts: 0,
            handoff: false,
            note_written: true,
            phase: Phase::Idle,
        }
    }

    /// The marker existed at the start: the rotation is pending from the first quiet
    /// moment, once `not_before` (the pause after a failed attempt) has passed;
    /// `attempts` is the count spent so far and `handoff` whether one of them wrote
    /// the note.
    pub fn pending(
        mut self,
        now: Instant,
        not_before: Option<Instant>,
        attempts: u32,
        handoff: bool,
    ) -> Self {
        if !self.limits.enabled {
            return self;
        }
        self.pending = true;
        self.not_before = not_before;
        self.attempts = attempts;
        self.handoff = handoff;
        let from = not_before.map_or(now, |t| t.max(now));
        self.phase = Phase::Waiting {
            deadline: from + self.limits.quiet,
        };
        self
    }

    /// The session was resumed with the combined cut-and-handoff line: the handoff
    /// turn is in flight; `attempts` counts the cut, `handoff` whether an earlier
    /// attempt wrote the note.
    pub fn retrying(mut self, now: Instant, attempts: u32, handoff: bool) -> Self {
        self.pending = true;
        self.attempts = attempts;
        self.handoff = handoff;
        self.phase = Phase::Handoff {
            deadline: now + self.limits.handoff,
        };
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

    /// The handoff has been requested and its turn has not ended.
    pub fn awaiting_handoff(&self) -> bool {
        matches!(self.phase, Phase::Handoff { .. })
    }

    /// What the disk says before a `result` while awaiting the handoff: whether the note
    /// has been written since the request. Without this call every turn end counts.
    pub fn set_note_written(&mut self, written: bool) {
        self.note_written = written;
    }

    /// The background tasks still running, for the journal: Claude Code's level
    /// signal replaces the set whenever it arrives.
    pub fn agents(&self) -> &BTreeSet<String> {
        &self.agents
    }

    /// The marker was removed by hand: a rotation still waiting for its quiet moment is
    /// cancelled; one whose handoff is under way goes on. Says whether it cancelled.
    pub fn cancel(&mut self) -> bool {
        if !self.pending || !matches!(self.phase, Phase::Waiting { .. }) {
            return false;
        }
        self.done();
        true
    }

    /// The rotation is over and the session goes on.
    fn done(&mut self) {
        self.pending = false;
        self.not_before = None;
        self.attempts = 0;
        self.handoff = false;
        self.phase = Phase::Idle;
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
            Event::Assistant {
                subagent,
                context_tokens,
                synthetic,
            } => {
                self.turn = true;
                // A message Claude Code wrote itself (an API error) has no prompt size.
                if let (false, false, Some(n)) = (subagent, synthetic, context_tokens) {
                    self.context = *n;
                }
            }
            Event::User { person } => {
                self.turn = true;
                if *person {
                    self.last_person = now;
                }
            }
            Event::Result { is_error, person } => {
                self.turn = false;
                if *person {
                    self.last_person = now;
                }
                match self.phase {
                    Phase::Handoff { .. } => {
                        if !*is_error && !self.note_written {
                            return vec![Action::AwaitHandoff];
                        }
                        if *is_error {
                            return self.failed(Step::Handoff, now);
                        }
                        self.handoff = true;
                        self.phase = Phase::Compacting {
                            deadline: now + self.limits.compact,
                            boundary: false,
                            failed: false,
                        };
                        return vec![Action::Snapshot(Moment::AfterHandoff), Action::Compact];
                    }
                    Phase::Compacting {
                        boundary, failed, ..
                    } => {
                        if boundary {
                            self.done();
                            return vec![Action::Compacted];
                        }
                        if failed || *is_error {
                            return self.failed(Step::Compaction, now);
                        }
                        // A person's turn that was queued before the command ended first.
                        return vec![Action::AwaitCompaction];
                    }
                    Phase::Idle | Phase::Waiting { .. } => {}
                }
            }
            Event::BackgroundTasks { ids } => {
                self.agents = ids.iter().cloned().collect();
            }
            Event::Compacted { auto } => {
                // The next main-line assistant message says how small it got.
                self.context = 0;
                if let (false, Phase::Compacting { boundary, .. }) = (auto, &mut self.phase) {
                    *boundary = true;
                }
            }
            Event::CompactionFailed => {
                if let Phase::Compacting { failed, .. } = &mut self.phase {
                    *failed = true;
                }
            }
            Event::Init { .. }
            | Event::ContextUsage { .. }
            | Event::ModelFallback { .. }
            | Event::Other => {}
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
            && now.duration_since(self.last_person) >= self.limits.idle
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
            self.phase = Phase::Waiting {
                deadline: now + self.limits.quiet,
            };
        }
    }

    fn advance(&mut self, now: Instant) -> Vec<Action> {
        match self.phase {
            Phase::Idle => Vec::new(),
            Phase::Waiting { deadline } => {
                if self.not_before.is_some_and(|t| now < t) {
                    Vec::new()
                } else if self.quiet() {
                    self.phase = Phase::Handoff {
                        deadline: now + self.limits.handoff,
                    };
                    // A retried handoff keeps the clean copy of the first attempt.
                    if self.attempts == 0 {
                        vec![
                            Action::Snapshot(Moment::BeforeHandoff),
                            Action::RequestHandoff,
                        ]
                    } else {
                        vec![Action::RequestHandoff]
                    }
                } else if now >= deadline {
                    self.cut(Step::Wait)
                } else {
                    Vec::new()
                }
            }
            Phase::Handoff { deadline } => {
                if now >= deadline {
                    self.cut(Step::Handoff)
                } else {
                    Vec::new()
                }
            }
            Phase::Compacting { deadline, .. } => {
                if now >= deadline {
                    self.cut(Step::Compaction)
                } else {
                    Vec::new()
                }
            }
        }
    }

    /// A cap expired: a cut-and-resume, or the end of the rotation.
    fn cut(&mut self, step: Step) -> Vec<Action> {
        self.phase = Phase::Idle;
        self.attempts += 1;
        if self.attempts < HANDOFF_ATTEMPTS {
            vec![Action::Retry {
                step,
                attempts: self.attempts,
                handoff: self.handoff,
            }]
        } else {
            vec![Action::GiveUp {
                step,
                cut: true,
                handoff: self.handoff,
            }]
        }
    }

    /// A handoff turn or a compaction failed: another attempt after the pause, or the
    /// end of the rotation.
    fn failed(&mut self, step: Step, now: Instant) -> Vec<Action> {
        self.phase = Phase::Idle;
        self.attempts += 1;
        if self.attempts < HANDOFF_ATTEMPTS {
            self.not_before = Some(now + self.limits.retry_pause);
            vec![Action::Postpone {
                step,
                attempts: self.attempts,
                handoff: self.handoff,
            }]
        } else {
            vec![Action::GiveUp {
                step,
                cut: false,
                handoff: self.handoff,
            }]
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
            compact: Duration::from_secs(2700),
            retry_pause: Duration::from_secs(900),
        }
    }

    fn assistant(context: u64) -> Event {
        Event::Assistant {
            subagent: false,
            context_tokens: Some(context),
            synthetic: false,
        }
    }

    fn tasks(ids: &[&str]) -> Event {
        Event::BackgroundTasks {
            ids: ids.iter().map(|s| s.to_string()).collect(),
        }
    }

    const OK: Event = Event::Result {
        is_error: false,
        person: false,
    };
    const ERR: Event = Event::Result {
        is_error: true,
        person: false,
    };
    const PERSON: Event = Event::User { person: true };
    const TIMER: Event = Event::User { person: false };
    const BOUNDARY: Event = Event::Compacted { auto: false };
    const BEFORE: Action = Action::Snapshot(Moment::BeforeHandoff);
    const AFTER: Action = Action::Snapshot(Moment::AfterHandoff);

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    /// The marker seen at a quiet moment: the handoff is requested.
    fn requested(t0: Instant) -> Tracker {
        let mut tr = Tracker::new(limits(), t0);
        tr.event(&OK, t0);
        assert_eq!(
            tr.marker_seen(t0 + secs(1)),
            vec![BEFORE, Action::RequestHandoff]
        );
        tr
    }

    /// The handoff turn ended with the note written: the compaction is sent.
    fn compacting(t0: Instant) -> Tracker {
        let mut tr = requested(t0);
        tr.event(&assistant(50_000), t0 + secs(2));
        assert_eq!(tr.event(&OK, t0 + secs(60)), vec![AFTER, Action::Compact]);
        assert!(tr.is_pending());
        tr
    }

    #[test]
    fn threshold_needs_both_a_large_context_and_four_hours_without_a_person() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0);
        // A person writes and the mind answers with a large context.
        assert!(tr.event(&PERSON, t0 + secs(1)).is_empty());
        assert!(tr.event(&assistant(310_000), t0 + secs(2)).is_empty());
        assert!(tr.event(&OK, t0 + secs(3)).is_empty());
        // Big context, the person not away for long enough.
        assert!(tr.tick(t0 + secs(3600)).is_empty());
        // A timer wakeup two hours later is a turn but not the person: the gap goes on.
        tr.event(&TIMER, t0 + secs(2 * 3600));
        tr.event(&assistant(310_050), t0 + secs(2 * 3600 + 1));
        tr.event(&OK, t0 + secs(2 * 3600 + 2));
        assert!(tr.tick(t0 + secs(4 * 3600)).is_empty());
        // Four hours after the person's message: the marker, and the handoff at once
        // since it is quiet.
        assert_eq!(
            tr.tick(t0 + secs(1 + 4 * 3600)),
            vec![Action::MarkPending, BEFORE, Action::RequestHandoff]
        );
        // The handoff turn runs and ends: the compaction follows, as after any trigger.
        let t = t0 + secs(2 + 4 * 3600);
        assert!(tr.event(&assistant(310_100), t).is_empty());
        assert_eq!(tr.event(&OK, t + secs(60)), vec![AFTER, Action::Compact]);
    }

    #[test]
    fn a_persons_message_resets_the_gap() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0);
        tr.event(&assistant(310_000), t0 + secs(1));
        tr.event(&OK, t0 + secs(2));
        // Three hours in, the person writes: another four hours from there.
        tr.event(&PERSON, t0 + secs(3 * 3600));
        tr.event(&assistant(310_100), t0 + secs(3 * 3600 + 1));
        tr.event(&OK, t0 + secs(3 * 3600 + 2));
        assert!(tr.tick(t0 + secs(7 * 3600 - 1)).is_empty());
        assert_eq!(tr.tick(t0 + secs(7 * 3600))[0], Action::MarkPending);
    }

    #[test]
    fn a_channel_turns_result_resets_the_gap_without_its_user_line() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0);
        tr.event(&assistant(310_000), t0 + secs(3 * 3600));
        tr.event(
            &Event::Result {
                is_error: false,
                person: true,
            },
            t0 + secs(3 * 3600 + 2),
        );
        assert!(tr.tick(t0 + secs(7 * 3600)).is_empty());
        assert_eq!(tr.tick(t0 + secs(7 * 3600 + 2))[0], Action::MarkPending);
    }

    #[test]
    fn a_small_context_never_trips_and_a_running_turn_is_never_idle() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0);
        tr.event(&assistant(100_000), t0);
        tr.event(&OK, t0);
        assert!(tr.tick(t0 + secs(10 * 3600)).is_empty());
        // A subagent's prompt does not count.
        tr.event(
            &Event::Assistant {
                subagent: true,
                context_tokens: Some(900_000),
                synthetic: false,
            },
            t0,
        );
        tr.event(&OK, t0);
        assert!(tr.tick(t0 + secs(20 * 3600)).is_empty());
        // A synthetic message (an API error) neither raises nor resets the size.
        tr.event(&assistant(900_000), t0);
        tr.event(
            &Event::Assistant {
                subagent: false,
                context_tokens: Some(0),
                synthetic: true,
            },
            t0,
        );
        assert_eq!(tr.context(), 900_000);
        tr.event(&assistant(100_000), t0);
        tr.event(&OK, t0);
        assert!(tr.tick(t0 + secs(20 * 3600)).is_empty());
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
        tr.event(&tasks(&["a1"]), t0 + secs(2));
        assert!(tr.marker_seen(t0 + secs(3)).is_empty());
        assert!(tr.is_pending());
        // Turn ends, the agent still runs: keep waiting.
        assert!(tr.event(&OK, t0 + secs(10)).is_empty());
        assert_eq!(tr.agents().len(), 1);
        // The set is empty again: quiet.
        assert_eq!(
            tr.event(&tasks(&[]), t0 + secs(20)),
            vec![BEFORE, Action::RequestHandoff]
        );
        // A second marker sighting changes nothing.
        assert!(tr.marker_seen(t0 + secs(21)).is_empty());
    }

    #[test]
    fn the_level_signal_replaces_the_set() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0);
        tr.event(&tasks(&["a", "b"]), t0);
        tr.event(&tasks(&["b"]), t0);
        assert_eq!(tr.event(&OK, t0 + secs(1)), vec![]);
        assert_eq!(tr.agents().iter().collect::<Vec<_>>(), vec!["b"]);
        tr.event(&tasks(&[]), t0 + secs(2));
        assert!(tr.quiet());
        assert_eq!(
            tr.marker_seen(t0 + secs(3)),
            vec![BEFORE, Action::RequestHandoff]
        );
    }

    #[test]
    fn the_session_is_compacted_in_place() {
        let t0 = Instant::now();
        let mut tr = compacting(t0);
        // The compaction's own lines, then its result: the rotation is over, the
        // context is unknown until the next assistant line.
        assert!(tr.event(&TIMER, t0 + secs(61)).is_empty());
        assert!(tr.event(&BOUNDARY, t0 + secs(70)).is_empty());
        assert_eq!(tr.context(), 0);
        assert_eq!(tr.event(&OK, t0 + secs(71)), vec![Action::Compacted]);
        assert!(!tr.is_pending());
        assert!(!tr.awaiting_handoff());
        // The session goes on: the next marker starts a whole new rotation, with its
        // own clean snapshot.
        tr.event(&assistant(900), t0 + secs(80));
        tr.event(&OK, t0 + secs(81));
        assert!(
            tr.tick(t0 + secs(100 + 4 * 3600)).is_empty(),
            "the reset context does not trip the threshold"
        );
        assert_eq!(
            tr.marker_seen(t0 + secs(200 + 4 * 3600)),
            vec![BEFORE, Action::RequestHandoff]
        );
    }

    #[test]
    fn a_failed_compaction_is_retried_from_the_handoff_then_gives_way_to_a_fresh_id() {
        let t0 = Instant::now();
        // Claude Code reports the failure (an API error, a blocked compaction), then
        // ends the turn without an error result: postponed like a failed handoff turn.
        let mut tr = compacting(t0);
        assert!(tr.event(&Event::CompactionFailed, t0 + secs(61)).is_empty());
        tr.event(&assistant(50_000), t0 + secs(62));
        assert_eq!(
            tr.event(&OK, t0 + secs(63)),
            vec![Action::Postpone {
                step: Step::Compaction,
                attempts: 1,
                handoff: true
            }]
        );
        assert!(tr.is_pending());
        // The resumed run after the pause: the handoff again, with no clean snapshot,
        // then the compaction again; a second failure ends the rotation with a fresh
        // id and the handoff done.
        let t1 = t0 + secs(1000);
        let mut tr = Tracker::new(limits(), t1).pending(t1, Some(t1 + secs(900)), 1, true);
        tr.event(&OK, t1 + secs(5));
        assert_eq!(tr.tick(t1 + secs(900)), vec![Action::RequestHandoff]);
        tr.event(&assistant(50_000), t1 + secs(901));
        assert_eq!(tr.event(&OK, t1 + secs(930)), vec![AFTER, Action::Compact]);
        assert_eq!(
            tr.event(&ERR, t1 + secs(940)),
            vec![Action::GiveUp {
                step: Step::Compaction,
                cut: false,
                handoff: true
            }]
        );
        // An error result during the first compaction is postponed the same way.
        let mut tr = compacting(t0);
        assert_eq!(
            tr.event(&ERR, t0 + secs(63)),
            vec![Action::Postpone {
                step: Step::Compaction,
                attempts: 1,
                handoff: true
            }]
        );
        // A failed handoff turn and a failed compaction share the count: the second
        // attempt of either kind is the last. The note of the first attempt is this
        // rotation's, so the fresh session is still pointed at it.
        let mut tr = Tracker::new(limits(), t1).pending(t1, Some(t1 + secs(900)), 1, true);
        tr.event(&OK, t1 + secs(5));
        tr.tick(t1 + secs(900));
        assert_eq!(
            tr.event(&ERR, t1 + secs(930)),
            vec![Action::GiveUp {
                step: Step::Handoff,
                cut: false,
                handoff: true
            }]
        );
    }

    #[test]
    fn a_hanging_compaction_is_cut_and_resumed_once() {
        let t0 = Instant::now();
        // The compaction runs under its own cap, not the handoff's; past it, the one
        // cut-and-resume, which asks for the handoff again.
        let mut tr = compacting(t0);
        assert!(
            tr.tick(t0 + secs(60 + 900)).is_empty(),
            "the handoff cap does not bound the compaction"
        );
        assert!(tr.tick(t0 + secs(60 + 2699)).is_empty());
        assert_eq!(
            tr.tick(t0 + secs(60 + 2700)),
            vec![Action::Retry {
                step: Step::Compaction,
                attempts: 1,
                handoff: true
            }]
        );
        assert!(tr.tick(t0 + secs(60 + 2701)).is_empty(), "said once");
        // The resumed run: handoff, compaction, and a second hang ends the rotation
        // with a fresh id and the handoff done, not the give-up line.
        let t1 = t0 + secs(1000);
        let mut tr = Tracker::new(limits(), t1).retrying(t1, 1, true);
        tr.event(&assistant(50_000), t1 + secs(1));
        assert_eq!(tr.event(&OK, t1 + secs(120)), vec![AFTER, Action::Compact]);
        assert!(tr.tick(t1 + secs(120 + 2699)).is_empty());
        assert_eq!(
            tr.tick(t1 + secs(120 + 2700)),
            vec![Action::GiveUp {
                step: Step::Compaction,
                cut: true,
                handoff: true
            }]
        );
        // The retried compaction can also succeed.
        let mut tr = Tracker::new(limits(), t1).retrying(t1, 1, true);
        tr.event(&OK, t1 + secs(120));
        tr.event(&BOUNDARY, t1 + secs(130));
        assert_eq!(tr.event(&OK, t1 + secs(131)), vec![Action::Compacted]);
        assert!(!tr.is_pending());
    }

    #[test]
    fn a_turn_queued_before_the_compaction_does_not_end_it() {
        let t0 = Instant::now();
        let mut tr = compacting(t0);
        // A person's message was queued before the command: its turn ends first.
        tr.event(&assistant(50_100), t0 + secs(61));
        assert_eq!(tr.event(&OK, t0 + secs(70)), vec![Action::AwaitCompaction]);
        assert!(tr.is_pending());
        // Then the compaction.
        tr.event(&BOUNDARY, t0 + secs(80));
        assert_eq!(tr.event(&OK, t0 + secs(81)), vec![Action::Compacted]);
        // An automatic compaction in that window is not the one asked for.
        let mut tr = compacting(t0);
        tr.event(&Event::Compacted { auto: true }, t0 + secs(61));
        assert_eq!(tr.event(&OK, t0 + secs(62)), vec![Action::AwaitCompaction]);
    }

    #[test]
    fn the_quiet_cap_cuts_once_then_gives_up() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0);
        tr.event(&OK, t0);
        tr.event(&assistant(50_000), t0 + secs(1));
        tr.marker_seen(t0 + secs(2));
        assert!(tr.tick(t0 + secs(2) + secs(1799)).is_empty());
        // Cut before the handoff went out: the clean snapshot is still due.
        assert_eq!(
            tr.tick(t0 + secs(2) + secs(1800)),
            vec![Action::Retry {
                step: Step::Wait,
                attempts: 1,
                handoff: false
            }]
        );
        // The resumed run: the combined line is the handoff request.
        let t1 = t0 + secs(2000);
        let mut tr = Tracker::new(limits(), t1).retrying(t1, 1, false);
        tr.event(&assistant(50_000), t1 + secs(1));
        assert!(tr.tick(t1 + secs(899)).is_empty());
        assert_eq!(
            tr.tick(t1 + secs(900)),
            vec![Action::GiveUp {
                step: Step::Handoff,
                cut: true,
                handoff: false
            }]
        );
    }

    #[test]
    fn a_turn_without_the_note_does_not_end_the_handoff() {
        let t0 = Instant::now();
        let mut tr = requested(t0);
        assert!(tr.awaiting_handoff());
        // A person's message started a turn that ended first: keep waiting.
        tr.event(&assistant(50_000), t0 + secs(2));
        tr.set_note_written(false);
        assert_eq!(tr.event(&OK, t0 + secs(3)), vec![Action::AwaitHandoff]);
        assert!(tr.awaiting_handoff());
        // The handoff cap still counts from the request.
        assert!(tr.tick(t0 + secs(1) + secs(899)).is_empty());
        // The handoff turn ends with the note written: on to the compaction.
        tr.set_note_written(true);
        assert_eq!(tr.event(&OK, t0 + secs(4)), vec![AFTER, Action::Compact]);
        // An error result ends the handoff turn whatever the disk says.
        let mut tr = requested(t0);
        tr.set_note_written(false);
        assert_eq!(
            tr.event(&ERR, t0 + secs(2)),
            vec![Action::Postpone {
                step: Step::Handoff,
                attempts: 1,
                handoff: false
            }]
        );
    }

    #[test]
    fn the_retry_resume_can_end_normally() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0).retrying(t0, 1, false);
        tr.event(&assistant(50_000), t0 + secs(1));
        assert_eq!(tr.event(&OK, t0 + secs(120)), vec![AFTER, Action::Compact]);
    }

    #[test]
    fn the_handoff_cap_and_a_failed_handoff_turn() {
        let t0 = Instant::now();
        let mut tr = requested(t0);
        tr.event(&assistant(50_000), t0 + secs(2));
        assert_eq!(
            tr.tick(t0 + secs(1) + secs(900)),
            vec![Action::Retry {
                step: Step::Handoff,
                attempts: 1,
                handoff: false
            }]
        );

        let mut tr = requested(t0);
        assert_eq!(
            tr.event(&ERR, t0 + secs(30)),
            vec![Action::Postpone {
                step: Step::Handoff,
                attempts: 1,
                handoff: false
            }]
        );
    }

    #[test]
    fn the_second_handoff_attempt_takes_no_clean_snapshot_and_a_second_failure_gives_up() {
        let t0 = Instant::now();
        // The resumed run after one failure carries the count.
        let mut tr = Tracker::new(limits(), t0).pending(t0, Some(t0 + secs(900)), 1, false);
        tr.event(&OK, t0 + secs(5));
        assert_eq!(tr.tick(t0 + secs(900)), vec![Action::RequestHandoff]);
        tr.event(&assistant(50_000), t0 + secs(901));
        assert_eq!(
            tr.event(&ERR, t0 + secs(930)),
            vec![Action::GiveUp {
                step: Step::Handoff,
                cut: false,
                handoff: false
            }]
        );
    }

    #[test]
    fn cuts_and_failures_spend_the_same_attempts_across_restarts() {
        let t0 = Instant::now();
        // A handoff turn failed once (the count came back from disk); the next handoff
        // turn hangs: the cap does not resume a second time, it ends the rotation, and
        // no attempt wrote the note.
        let mut tr = Tracker::new(limits(), t0).pending(t0, Some(t0 + secs(900)), 1, false);
        tr.event(&OK, t0 + secs(5));
        assert_eq!(tr.tick(t0 + secs(900)), vec![Action::RequestHandoff]);
        tr.event(&assistant(50_000), t0 + secs(901));
        assert_eq!(
            tr.tick(t0 + secs(900 + 900)),
            vec![Action::GiveUp {
                step: Step::Handoff,
                cut: true,
                handoff: false
            }]
        );
        // A turn was cut once (the count came back from disk); the handoff turn of the
        // resumed session fails: no pause and third attempt, the rotation ends.
        let mut tr = Tracker::new(limits(), t0).retrying(t0, 1, false);
        assert_eq!(
            tr.event(&ERR, t0 + secs(30)),
            vec![Action::GiveUp {
                step: Step::Handoff,
                cut: false,
                handoff: false
            }]
        );
        // The same after the compaction was cut: the note of the first attempt is this
        // rotation's, and the fresh session is pointed at it.
        let mut tr = Tracker::new(limits(), t0).retrying(t0, 1, true);
        tr.event(&assistant(50_000), t0 + secs(1));
        assert_eq!(
            tr.event(&ERR, t0 + secs(30)),
            vec![Action::GiveUp {
                step: Step::Handoff,
                cut: false,
                handoff: true
            }]
        );
    }

    #[test]
    fn a_pending_start_honours_the_pause_then_rotates() {
        let t0 = Instant::now();
        let pause_end = t0 + secs(900);
        let mut tr = Tracker::new(limits(), t0).pending(t0, Some(pause_end), 0, false);
        // The start turn ends, but the pause is on.
        assert!(tr.event(&OK, t0 + secs(5)).is_empty());
        assert!(tr.tick(t0 + secs(899)).is_empty());
        assert_eq!(tr.tick(pause_end), vec![BEFORE, Action::RequestHandoff]);
        // The quiet cap counted from the end of the pause, not from the start.
        let mut tr = Tracker::new(limits(), t0).pending(t0, Some(pause_end), 0, false);
        tr.event(&assistant(1), t0 + secs(5));
        assert!(tr.tick(pause_end + secs(1799)).is_empty());
        assert_eq!(
            tr.tick(pause_end + secs(1800)),
            vec![Action::Retry {
                step: Step::Wait,
                attempts: 1,
                handoff: false
            }]
        );
    }

    #[test]
    fn removing_the_marker_cancels_a_waiting_rotation_but_not_a_handoff() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(limits(), t0);
        tr.event(&assistant(50_000), t0);
        tr.marker_seen(t0 + secs(1));
        assert!(tr.is_pending());
        assert!(tr.cancel());
        assert!(!tr.is_pending());
        // The turn ends: nothing happens, and the cap is gone with the rotation.
        assert!(tr.event(&OK, t0 + secs(2)).is_empty());
        assert!(tr.tick(t0 + secs(2 + 1800)).is_empty());
        // Once the handoff is requested it runs to its end.
        assert_eq!(
            tr.marker_seen(t0 + secs(3)),
            vec![BEFORE, Action::RequestHandoff]
        );
        assert!(!tr.cancel());
        assert_eq!(tr.event(&OK, t0 + secs(4)), vec![AFTER, Action::Compact]);
        assert!(!tr.cancel());
    }

    #[test]
    fn disabled_ignores_markers_and_thresholds() {
        let t0 = Instant::now();
        let mut tr = Tracker::new(
            Limits {
                enabled: false,
                ..limits()
            },
            t0,
        )
        .pending(t0, None, 0, false);
        tr.event(&assistant(900_000), t0);
        tr.event(&OK, t0);
        assert!(tr.marker_seen(t0 + secs(1)).is_empty());
        assert!(tr.tick(t0 + secs(10 * 3600)).is_empty());
        assert!(!tr.is_pending());
    }
}
