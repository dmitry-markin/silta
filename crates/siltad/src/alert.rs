//! Alerts to the owner from the daemon itself: a session away too long, a session
//! silent after a delivery, a session back. The bookkeeping is `silta::alert`; here
//! is the wording, the owner's DM, and the task that ticks.
//!
//! Sent by the daemon, never routed as an event (the daemon drops its own messages),
//! so no mind sees them. Sent from the daemon because it outlives every session and
//! holds the Matrix device: the thing that is broken must not be the thing that
//! reports it.

use std::time::Duration;

use matrix_sdk::{ruma::events::room::message::RoomMessageEventContent, Room};
use silta::{alert::Alert, protocol::Role, time::rfc3339_utc};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    daemon::{now_ms, Daemon, Shared},
    outbound::member_ids,
};

/// A delivered message that saw no visible action within the typing cap.
#[derive(Debug)]
pub struct Silence {
    pub session: String,
    pub room_id: String,
    pub delivered_ms: u64,
}

const TICK: Duration = Duration::from_secs(30);

/// Send the away alerts as they come due, and the silent ones as the typing tasks
/// report them.
pub fn spawn_watcher(
    daemon: Shared,
    mut silence: mpsc::UnboundedReceiver<Silence>,
    cancel: CancellationToken,
) {
    if !daemon.alerts.lock().unwrap().enabled() {
        info!("owner alerts are off (alert_grace_secs = 0)");
        return;
    }
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(TICK);
        loop {
            let due = tokio::select! {
                _ = cancel.cancelled() => return,
                _ = ticks.tick() => daemon.alerts.lock().unwrap().tick(now_ms()),
                report = silence.recv() => match report {
                    Some(s) => daemon.alerts.lock().unwrap().silent(&s.session, &s.room_id, s.delivered_ms, now_ms()).into_iter().collect(),
                    None => return,
                },
            };
            for alert in due {
                send(&daemon, alert).await;
            }
        }
    });
}

/// Post one alert to the owner's DM; logged either way.
pub async fn send(daemon: &Daemon, alert: Alert) {
    let text = message(&alert, &daemon.hostname, now_ms());
    match alert {
        Alert::Back { .. } => info!("{text}"),
        _ => warn!("{text}"),
    }
    match owner_dm(daemon).await {
        Some(room) => {
            if let Err(err) = room.send(RoomMessageEventContent::text_plain(&text)).await {
                warn!(room = %room.room_id(), "cannot send the alert to the owner's DM: {err}");
            }
        }
        None => warn!("no DM with an owner to send the alert to"),
    }
}

/// The joined room whose members other than the bot are all addresses of one person
/// with the owner role, and at least one; the first found.
async fn owner_dm(daemon: &Daemon) -> Option<Room> {
    for room in daemon.client.joined_rooms() {
        let Ok(members) = member_ids(&room).await else {
            continue;
        };
        let mut owner: Option<&str> = None;
        let mut only_owner = true;
        for member in members.iter().filter(|m| !daemon.routing.is_bot(m)) {
            match daemon.routing.person_for(member) {
                Some(person)
                    if person.role == Role::Owner && owner.is_none_or(|o| o == person.name) =>
                {
                    owner = Some(&person.name);
                }
                _ => {
                    only_owner = false;
                    break;
                }
            }
        }
        if only_owner && owner.is_some() {
            return Some(room);
        }
    }
    None
}

/// The text of an alert; plain, with the unit and the host named so the owner knows
/// where to look.
pub fn message(alert: &Alert, host: &str, now_ms: u64) -> String {
    match alert {
        Alert::Away { session, since_ms } => format!(
            "⚠ Silta: session \"{session}\" has been disconnected for {} (since {}). Check \
             \"systemctl status silta-session@{session}\" and \"journalctl -u silta-session@{session}\" \
             on {host}; a failed start or a missing or expired token are the usual causes.",
            minutes(now_ms.saturating_sub(*since_ms)),
            rfc3339_utc(*since_ms)
        ),
        Alert::Back { session, away_ms } => {
            format!("Silta: session \"{session}\" is connected again after {}.", minutes(*away_ms))
        }
        Alert::Silent { session, delivered_ms, .. } => format!(
            "⚠ Silta: session \"{session}\" received a message at {} and has shown no reply, reaction \
             or typing for {}. If it repeats, check \"journalctl -u silta-session@{session}\" on {host}: \
             an expired token shows as authentication_failed, an API account out of credits as an API error.",
            rfc3339_utc(*delivered_ms),
            minutes(now_ms.saturating_sub(*delivered_ms))
        ),
    }
}

fn minutes(ms: u64) -> String {
    let minutes = ms / 60_000;
    if minutes < 60 {
        format!("{minutes} min")
    } else if minutes.is_multiple_of(60) {
        format!("{} h", minutes / 60)
    } else {
        format!("{} h {} min", minutes / 60, minutes % 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wording_names_the_unit_the_host_and_the_times() {
        let t0 = 1_788_631_331_000; // 2026-09-05T18:02:11Z
        let away = message(
            &Alert::Away {
                session: "alice".into(),
                since_ms: t0,
            },
            "myhost",
            t0 + 25 * 60_000,
        );
        assert!(away.starts_with("⚠ Silta: session \"alice\" has been disconnected for 25 min (since 2026-09-05T18:02:11Z)"));
        assert!(away.contains("journalctl -u silta-session@alice\" on myhost"));
        let back = message(
            &Alert::Back {
                session: "hub".into(),
                away_ms: 90 * 60_000,
            },
            "myhost",
            t0,
        );
        assert_eq!(
            back,
            "Silta: session \"hub\" is connected again after 1 h 30 min."
        );
        let silent = message(
            &Alert::Silent {
                session: "alice".into(),
                room_id: "!r".into(),
                delivered_ms: t0,
            },
            "myhost",
            t0 + 600_000,
        );
        assert!(silent.contains("received a message at 2026-09-05T18:02:11Z and has shown no reply, reaction or typing for 10 min"));
        assert_eq!(minutes(2 * 3_600_000), "2 h");
    }
}
