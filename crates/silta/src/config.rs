//! `siltad` configuration: the people registry, the sessions, and the pure routing
//! rules derived from them.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::{Path, PathBuf},
};

use serde::{
    de::{self, MapAccess, Visitor},
    Deserialize, Deserializer,
};
use thiserror::Error;

use crate::protocol::{Person, Role};

/// The daemon configuration file (`siltad.toml`).
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// The daemon's Unix socket; the sessions' plugins connect here.
    #[serde(default = "default_socket")]
    pub socket: PathBuf,
    /// The store, the saved session, the delivery watermarks and the spool.
    #[serde(default = "default_state_dir")]
    pub state_dir: PathBuf,
    pub matrix: MatrixConfig,
    #[serde(default)]
    pub people: Vec<PersonConfig>,
    #[serde(default)]
    pub sessions: Vec<SessionConfig>,
    /// After a start, messages from before it are still delivered if they are at most
    /// this old and were not delivered before; older history is skipped. Also the
    /// maximum age of a message queued for a session that is not connected.
    #[serde(default = "default_replay_window_secs")]
    pub replay_window_secs: u64,
    /// Downloaded attachments older than this are deleted from the inbox.
    #[serde(default = "default_inbox_max_age_days")]
    pub inbox_max_age_days: u64,
    /// The cap on one attachment in either direction: a larger file from a room is not
    /// downloaded (the message says so instead), a larger file from a session is refused.
    #[serde(default = "default_attachment_max_mb")]
    pub attachment_max_mb: u64,
    /// A session disconnected for longer than this is reported to the owner's DM, and
    /// so is one that shows no visible action after a delivered message. 0 turns the
    /// alerts off.
    #[serde(default = "default_alert_grace_secs")]
    pub alert_grace_secs: u64,
    /// Reserved for speech recognition; accepted and ignored with a warning.
    #[serde(default)]
    pub asr: Option<toml::Value>,
}

/// Where the package's units put the socket and the state.
pub const DEFAULT_SOCKET: &str = "/run/siltad/siltad.sock";
pub const DEFAULT_STATE_DIR: &str = "/var/lib/siltad";

fn default_socket() -> PathBuf {
    PathBuf::from(DEFAULT_SOCKET)
}

fn default_state_dir() -> PathBuf {
    PathBuf::from(DEFAULT_STATE_DIR)
}

fn default_alert_grace_secs() -> u64 {
    600
}

#[derive(Clone, Deserialize)]
pub struct MatrixConfig {
    pub homeserver_url: String,
    pub user_id: String,
    /// Used only for the first login, when no saved session exists.
    pub password: String,
    /// Fixed device id: the bot's one and only device.
    pub device_id: String,
    #[serde(default = "default_device_name")]
    pub device_name: String,
    /// The account's display name, what clients show next to the bot's messages; set
    /// at every start when the profile differs. Absent: the profile is left alone.
    #[serde(default)]
    pub display_name: Option<String>,
}

fn default_device_name() -> String {
    "Silta".to_owned()
}

fn default_replay_window_secs() -> u64 {
    300
}

fn default_inbox_max_age_days() -> u64 {
    30
}

fn default_attachment_max_mb() -> u64 {
    100
}

impl fmt::Debug for MatrixConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MatrixConfig")
            .field("homeserver_url", &self.homeserver_url)
            .field("user_id", &self.user_id)
            .field("password", &"<redacted>")
            .field("device_id", &self.device_id)
            .field("device_name", &self.device_name)
            .field("display_name", &self.display_name)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PersonConfig {
    pub name: String,
    pub role: Role,
    /// Matrix user ids for now; other transports later.
    pub addresses: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SessionConfig {
    pub name: String,
    pub receive: Receive,
    #[serde(default)]
    pub send: SendPolicy,
    /// The Unix user the session's plugin runs as; a `hello` from any other user is
    /// refused. Resolved to a uid when the daemon starts.
    pub user: String,
    /// Reserved for later; accepted and ignored with a warning.
    #[serde(default)]
    pub relay_permissions: Option<toml::Value>,
}

/// Which inbound rooms a session owns: everything not claimed by another session, the
/// group rooms (two or more registered people in them), or an explicit list of people
/// and room ids. A listed room always goes to the session listing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Receive {
    All,
    Groups,
    Selective { people: Vec<String>, rooms: Vec<String> },
}

impl<'de> Deserialize<'de> for Receive {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ReceiveVisitor;

        impl<'de> Visitor<'de> for ReceiveVisitor {
            type Value = Receive;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str(r#"the string "all" or "groups", or a table { people = [...], rooms = [...] }"#)
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Receive, E> {
                if v == "all" {
                    Ok(Receive::All)
                } else if v == "groups" {
                    Ok(Receive::Groups)
                } else {
                    Err(E::invalid_value(de::Unexpected::Str(v), &self))
                }
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Receive, A::Error> {
                let mut people = None;
                let mut rooms = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "people" => people = Some(map.next_value()?),
                        "rooms" => rooms = Some(map.next_value()?),
                        other => return Err(de::Error::unknown_field(other, &["people", "rooms"])),
                    }
                }
                Ok(Receive::Selective {
                    people: people.unwrap_or_default(),
                    rooms: rooms.unwrap_or_default(),
                })
            }
        }

        deserializer.deserialize_any(ReceiveVisitor)
    }
}

/// Which rooms a session may write to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SendPolicy {
    /// Every room the bot is joined to.
    Any,
    /// Rooms the session lists, and rooms whose members (other than the bot) are all
    /// people the session receives from.
    #[default]
    Own,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read {path}: {source}")]
    Io { path: PathBuf, source: std::io::Error },
    #[error("cannot parse {path}: {source}")]
    Parse { path: PathBuf, source: toml::de::Error },
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

impl Config {
    /// Read, parse and validate a configuration file.
    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let text = std::fs::read_to_string(path)
            .map_err(|source| ConfigError::Io { path: path.to_owned(), source })?;
        Config::parse(&text).map_err(|err| match err {
            ConfigError::Parse { source, .. } => ConfigError::Parse { path: path.to_owned(), source },
            other => other,
        })
    }

    /// Parse and validate configuration text.
    pub fn parse(text: &str) -> Result<Config, ConfigError> {
        let config: Config =
            toml::from_str(text).map_err(|source| ConfigError::Parse { path: PathBuf::new(), source })?;
        config.validate()?;
        Ok(config)
    }

    /// All checks are fatal: a daemon must not start on an ambiguous registry.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Invalid(msg));

        if self.socket.as_os_str().is_empty() {
            return invalid("socket must be set".into());
        }
        if self.state_dir.as_os_str().is_empty() {
            return invalid("state_dir must be set".into());
        }
        let m = &self.matrix;
        if m.homeserver_url.is_empty() {
            return invalid("matrix.homeserver_url must be set".into());
        }
        if !is_user_id(&m.user_id) {
            return invalid(format!("matrix.user_id {:?} is not a Matrix user id", m.user_id));
        }
        if m.password.is_empty() {
            return invalid("matrix.password must be set".into());
        }
        if m.device_id.is_empty() {
            return invalid("matrix.device_id must be set".into());
        }
        if self.inbox_max_age_days == 0 {
            return invalid("inbox_max_age_days must be at least 1".into());
        }
        if self.attachment_max_mb == 0 {
            return invalid("attachment_max_mb must be at least 1".into());
        }

        let mut names = HashSet::new();
        let mut addresses = HashSet::new();
        for person in &self.people {
            if person.name.is_empty() {
                return invalid("a person has an empty name".into());
            }
            if !names.insert(person.name.as_str()) {
                return invalid(format!("person {:?} is listed twice", person.name));
            }
            for address in &person.addresses {
                if !is_user_id(address) {
                    return invalid(format!("address {address:?} of {:?} is not a Matrix user id", person.name));
                }
                if address == &m.user_id {
                    return invalid(format!("person {:?} has the bot's own user id as address", person.name));
                }
                if !addresses.insert(address.as_str()) {
                    return invalid(format!("address {address:?} is listed twice"));
                }
            }
        }

        let mut session_names = HashSet::new();
        let mut all_session: Option<&str> = None;
        let mut groups_session: Option<&str> = None;
        let mut room_owner: HashMap<&str, &str> = HashMap::new();
        let mut person_owner: HashMap<&str, &str> = HashMap::new();
        for session in &self.sessions {
            if session.name.is_empty() {
                return invalid("a session has an empty name".into());
            }
            if !session_names.insert(session.name.as_str()) {
                return invalid(format!("session {:?} is listed twice", session.name));
            }
            if session.user.is_empty() {
                return invalid(format!("session {:?} has no user", session.name));
            }
            match &session.receive {
                Receive::Groups => {
                    if let Some(first) = groups_session {
                        return invalid(format!(
                            "sessions {first:?} and {:?} both have receive = \"groups\"; only one may",
                            session.name
                        ));
                    }
                    groups_session = Some(&session.name);
                }
                Receive::All => {
                    if let Some(first) = all_session {
                        return invalid(format!(
                            "sessions {first:?} and {:?} both have receive = \"all\"; only one may",
                            session.name
                        ));
                    }
                    all_session = Some(&session.name);
                }
                Receive::Selective { people, rooms } => {
                    for person in people {
                        if !names.contains(person.as_str()) {
                            return invalid(format!(
                                "session {:?} receives from unknown person {person:?}",
                                session.name
                            ));
                        }
                        if let Some(other) = person_owner.insert(person, &session.name) {
                            return invalid(format!(
                                "person {person:?} is listed by sessions {other:?} and {:?}",
                                session.name
                            ));
                        }
                    }
                    for room in rooms {
                        if !room.starts_with('!') {
                            return invalid(format!("session {:?} lists {room:?}, which is not a room id", session.name));
                        }
                        if let Some(other) = room_owner.insert(room, &session.name) {
                            return invalid(format!(
                                "room {room:?} is listed by sessions {other:?} and {:?}",
                                session.name
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Non-fatal observations for the startup log.
    pub fn warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        if self.asr.is_some() {
            warnings.push("[asr] is reserved and ignored".into());
        }
        for session in &self.sessions {
            if session.relay_permissions.is_some() {
                warnings.push(format!(
                    "sessions.{}.relay_permissions is reserved and ignored",
                    session.name
                ));
            }
        }
        if self.sessions.is_empty() {
            warnings.push("no [[sessions]] configured: every inbound message will be dropped".into());
        }
        if self.people.is_empty() {
            warnings.push("no [[people]] configured: every inbound message will be dropped".into());
        }
        for person in &self.people {
            if person.addresses.is_empty() {
                warnings.push(format!("person {:?} has no addresses", person.name));
            }
        }
        warnings
    }
}

fn is_user_id(s: &str) -> bool {
    s.starts_with('@') && s[1..].contains(':') && !s.ends_with(':')
}

/// Why an inbound message was not delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// Sent by the bot itself.
    OwnMessage,
    /// The sender is not in the registry.
    UnknownSender,
    /// No session owns this room or person (or, for a group room, no session receives
    /// groups), and there is no `all` session.
    NoSession,
}

impl fmt::Display for DropReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DropReason::OwnMessage => "own message",
            DropReason::UnknownSender => "sender not registered",
            DropReason::NoSession => "no session owns the room",
        })
    }
}

/// The routing decision for one inbound message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inbound<'a> {
    Deliver { session: &'a str, person: &'a PersonConfig },
    Drop(DropReason),
}

/// Why a send was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendDenied {
    UnknownSession,
    NotAllowed,
}

/// Routing tables derived from a validated [`Config`]. Pure: the daemon supplies
/// sender, room and member lists, this decides.
#[derive(Debug, Clone)]
pub struct Routing {
    bot: String,
    people: Vec<PersonConfig>,
    by_address: HashMap<String, usize>,
    sessions: Vec<SessionConfig>,
    by_name: HashMap<String, usize>,
    by_room: HashMap<String, usize>,
    by_person: HashMap<String, usize>,
    all: Option<usize>,
    groups: Option<usize>,
}

/// What the daemon knows about a room besides its members: whether the bot's account
/// marks it as a DM (the inviting client's `is_direct`, which the SDK keeps in the
/// bot's `m.direct` when it joins), and whether it has a name or an alias, which
/// clients give to rooms and never to DMs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoomShape {
    pub direct: bool,
    pub named: bool,
}

impl RoomShape {
    pub const fn default_const() -> RoomShape {
        RoomShape { direct: false, named: false }
    }
    pub const DM: RoomShape = RoomShape { direct: true, named: false };
    pub const NAMED: RoomShape = RoomShape { direct: false, named: true };
}

/// Group or one person's room. Two or more registered people make a group whatever
/// the flags say. With one person the DM flag decides when it is there; without it a
/// named room is a group and an unnamed one a DM: a two-person room created as a room
/// carries a name, a DM from a client that forgot the flag carries none, and of the
/// two possible mistakes a nameless group answered by the person's own mind is the
/// harmless one, a DM answered by the shared hub is not (decided 2026-09-06).
fn is_group(people: usize, shape: RoomShape) -> bool {
    people >= 2 || (!shape.direct && shape.named)
}

impl Routing {
    pub fn new(config: &Config) -> Routing {
        let mut by_address = HashMap::new();
        for (i, person) in config.people.iter().enumerate() {
            for address in &person.addresses {
                by_address.insert(address.clone(), i);
            }
        }
        let mut by_name = HashMap::new();
        let mut by_room = HashMap::new();
        let mut by_person = HashMap::new();
        let mut all = None;
        let mut groups = None;
        for (i, session) in config.sessions.iter().enumerate() {
            by_name.insert(session.name.clone(), i);
            match &session.receive {
                Receive::All => all = Some(i),
                Receive::Groups => groups = Some(i),
                Receive::Selective { people, rooms } => {
                    for person in people {
                        by_person.insert(person.clone(), i);
                    }
                    for room in rooms {
                        by_room.insert(room.clone(), i);
                    }
                }
            }
        }
        Routing {
            bot: config.matrix.user_id.clone(),
            people: config.people.clone(),
            by_address,
            sessions: config.sessions.clone(),
            by_name,
            by_room,
            by_person,
            all,
            groups,
        }
    }

    pub fn bot_user_id(&self) -> &str {
        &self.bot
    }

    pub fn is_bot(&self, address: &str) -> bool {
        address == self.bot
    }

    /// The registered person behind an address.
    pub fn person_for(&self, address: &str) -> Option<&PersonConfig> {
        self.by_address.get(address).map(|&i| &self.people[i])
    }

    pub fn session(&self, name: &str) -> Option<&SessionConfig> {
        self.by_name.get(name).map(|&i| &self.sessions[i])
    }

    pub fn session_names(&self) -> impl Iterator<Item = &str> {
        self.sessions.iter().map(|s| s.name.as_str())
    }

    /// Every session with the Unix user it runs as.
    pub fn session_users(&self) -> impl Iterator<Item = (&str, &str)> {
        self.sessions.iter().map(|s| (s.name.as_str(), s.user.as_str()))
    }

    /// The people announced to a session in `welcome`: the ones it receives from, so a
    /// mind does not learn about other people from the handshake. A session that
    /// receives groups, everything, or listed rooms hears about everyone.
    pub fn people_for(&self, session: &str) -> Vec<Person> {
        let listed: Option<&Vec<String>> = match self.session(session).map(|s| &s.receive) {
            Some(Receive::Selective { people, rooms }) if rooms.is_empty() => Some(people),
            _ => None,
        };
        self.people
            .iter()
            .filter(|p| listed.is_none_or(|l| l.contains(&p.name)))
            .map(|p| Person { name: p.name.clone(), role: p.role })
            .collect()
    }

    /// The registered people among a room's members other than the bot, and whether
    /// every such member is registered.
    fn people_in<'m>(&self, members: impl IntoIterator<Item = &'m str>) -> (HashSet<&str>, bool) {
        let mut people = HashSet::new();
        let mut all_registered = true;
        for member in members {
            if self.is_bot(member) {
                continue;
            }
            match self.person_for(member) {
                Some(person) => {
                    people.insert(person.name.as_str());
                }
                None => all_registered = false,
            }
        }
        (people, all_registered)
    }

    /// Decide who gets an inbound message. `members` are the user ids in the room. The
    /// session listing the room wins; otherwise a group room (see [`is_group`]) goes
    /// to the `groups` session and a DM to the person's session; the `all` session
    /// takes what is left, and without one the message is dropped.
    pub fn inbound<'m>(
        &self,
        sender: &str,
        room_id: &str,
        members: impl IntoIterator<Item = &'m str>,
        shape: RoomShape,
    ) -> Inbound<'_> {
        if self.is_bot(sender) {
            return Inbound::Drop(DropReason::OwnMessage);
        }
        let Some(person) = self.person_for(sender) else {
            return Inbound::Drop(DropReason::UnknownSender);
        };
        let owner = match self.by_room.get(room_id) {
            Some(&i) => Some(i),
            None => {
                let (people, _) = self.people_in(members);
                let specific = if is_group(people.len(), shape) { self.groups } else { self.by_person.get(&person.name).copied() };
                specific.or(self.all)
            }
        };
        match owner {
            Some(i) => Inbound::Deliver { session: &self.sessions[i].name, person },
            None => Inbound::Drop(DropReason::NoSession),
        }
    }

    /// Apply a session's send policy to a room. `members` are the user ids currently in
    /// the room; the bot's own id is ignored if present.
    pub fn may_send<'m>(
        &self,
        session: &str,
        room_id: &str,
        members: impl IntoIterator<Item = &'m str>,
        shape: RoomShape,
    ) -> Result<(), SendDenied> {
        let Some(&i) = self.by_name.get(session) else {
            return Err(SendDenied::UnknownSession);
        };
        match self.sessions[i].send {
            SendPolicy::Any => Ok(()),
            SendPolicy::Own => self.owns_room(i, room_id, members, shape),
        }
    }

    /// Whether a session may read a room's history: the `all` session reads any room,
    /// another session the rooms it owns. Never the send policy, so a hub that may
    /// write into every room still cannot read another mind's rooms.
    pub fn may_read<'m>(
        &self,
        session: &str,
        room_id: &str,
        members: impl IntoIterator<Item = &'m str>,
        shape: RoomShape,
    ) -> Result<(), SendDenied> {
        let Some(&i) = self.by_name.get(session) else {
            return Err(SendDenied::UnknownSession);
        };
        if self.all == Some(i) {
            return Ok(());
        }
        self.owns_room(i, room_id, members, shape)
    }

    /// A session owns a room it lists, and any room whose members other than the bot
    /// are all registered people it receives from: everyone for `all`, the group rooms
    /// for `groups`, the DMs of the listed people otherwise (the same rule as the
    /// routing, so the hub owns a named two-person room and the person's mind does not).
    fn owns_room<'m>(&self, i: usize, room_id: &str, members: impl IntoIterator<Item = &'m str>, shape: RoomShape) -> Result<(), SendDenied> {
        if self.by_room.get(room_id) == Some(&i) {
            return Ok(());
        }
        let (people, all_registered) = self.people_in(members);
        if people.is_empty() || !all_registered {
            return Err(SendDenied::NotAllowed);
        }
        let group = is_group(people.len(), shape);
        let owned = match &self.sessions[i].receive {
            Receive::All => true,
            Receive::Groups => group,
            Receive::Selective { people: listed, .. } => !group && people.iter().all(|p| listed.iter().any(|l| l == p)),
        };
        if owned {
            Ok(())
        } else {
            Err(SendDenied::NotAllowed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No DM flag, no name: the members alone decide, as before the shape existed.
    const PLAIN: RoomShape = RoomShape::default_const();

    const BASE: &str = r#"
socket    = "/run/silta/siltad.sock"
state_dir = "/var/lib/silta"

[matrix]
homeserver_url = "http://127.0.0.1"
user_id        = "@silta:silta.test"
password       = "secret"
device_id      = "silta"
device_name    = "Silta"

[[people]]
name = "Bob"
role = "owner"
addresses = ["@bob:silta.test"]

[[people]]
name = "Alice"
role = "family"
addresses = ["@alice:silta.test", "@alice2:silta.test"]
"#;

    fn config(sessions: &str) -> Config {
        Config::parse(&format!("{BASE}\n{sessions}")).expect("valid config")
    }

    fn parse_err(sessions: &str) -> String {
        match Config::parse(&format!("{BASE}\n{sessions}")) {
            Err(ConfigError::Invalid(msg)) => msg,
            Err(other) => panic!("expected Invalid, got {other}"),
            Ok(_) => panic!("expected an error"),
        }
    }

    const HUB_ONLY: &str = r#"
[[sessions]]
name = "hub"
receive = "all"
send = "any"
user = "silta-hub"
"#;

    const SPLIT: &str = r#"
[[sessions]]
name = "hub"
receive = { people = ["Bob"], rooms = ["!family:silta.test"] }
send = "any"
user = "silta-hub"

[[sessions]]
name = "alice"
receive = { people = ["Alice"] }
send = "own"
user = "silta-alice"
"#;

    /// The production shape: minds per person, the hub for group rooms.
    const MINDS: &str = r#"
[[sessions]]
name = "hub"
receive = "groups"
send = "own"
user = "silta-hub"

[[sessions]]
name = "bob"
receive = { people = ["Bob"] }
user = "silta-bob"

[[sessions]]
name = "alice"
receive = { people = ["Alice"] }
user = "silta-alice"
"#;

    #[test]
    fn socket_state_dir_and_alert_grace_default_to_the_package_paths() {
        // BASE sets both paths; without them the package's paths apply.
        let without_paths: String = BASE.lines().filter(|l| !l.starts_with("socket") && !l.starts_with("state_dir")).collect::<Vec<_>>().join("\n");
        let c = Config::parse(&format!("{without_paths}\n{HUB_ONLY}")).unwrap();
        assert_eq!(c.socket, PathBuf::from(DEFAULT_SOCKET));
        assert_eq!(c.state_dir, PathBuf::from(DEFAULT_STATE_DIR));
        assert_eq!(c.alert_grace_secs, 600);
        let c = config(HUB_ONLY);
        assert_eq!(c.socket, PathBuf::from("/run/silta/siltad.sock"));
        let c = Config::parse(&format!("alert_grace_secs = 0\n{BASE}\n{HUB_ONLY}")).unwrap();
        assert_eq!(c.alert_grace_secs, 0);
    }

    #[test]
    fn display_name_is_optional() {
        assert_eq!(config(HUB_ONLY).matrix.display_name, None);
        let text = format!("{BASE}\n{HUB_ONLY}").replace("device_name    = \"Silta\"", "device_name = \"d\"\ndisplay_name = \"Silta\"");
        assert_eq!(Config::parse(&text).unwrap().matrix.display_name.as_deref(), Some("Silta"));
    }

    #[test]
    fn replay_window_default_and_override() {
        assert_eq!(config(HUB_ONLY).replay_window_secs, 300);
        let c = Config::parse(&format!("replay_window_secs = 30\n{BASE}\n{HUB_ONLY}")).unwrap();
        assert_eq!(c.replay_window_secs, 30);
    }

    #[test]
    fn inbox_defaults_and_limits() {
        let c = config(HUB_ONLY);
        assert_eq!(c.inbox_max_age_days, 30);
        assert_eq!(c.attachment_max_mb, 100);
        let c = Config::parse(&format!("inbox_max_age_days = 7\nattachment_max_mb = 5\n{BASE}\n{HUB_ONLY}")).unwrap();
        assert_eq!((c.inbox_max_age_days, c.attachment_max_mb), (7, 5));
        for bad in ["inbox_max_age_days = 0", "attachment_max_mb = 0"] {
            match Config::parse(&format!("{bad}\n{BASE}\n{HUB_ONLY}")) {
                Err(ConfigError::Invalid(msg)) => assert!(msg.contains("at least 1"), "{msg}"),
                other => panic!("expected Invalid for {bad}, got {other:?}"),
            }
        }
    }

    #[test]
    fn parses_the_plan_example() {
        let c = config(HUB_ONLY);
        assert_eq!(c.sessions[0].receive, Receive::All);
        assert_eq!(c.sessions[0].send, SendPolicy::Any);
        assert_eq!(c.people[1].role, Role::Family);
        assert_eq!(c.sessions[0].user, "silta-hub");
        assert!(c.warnings().is_empty(), "{:?}", c.warnings());
        assert!(!format!("{:?}", c.matrix).contains("secret"));
    }

    #[test]
    fn parses_the_split_form() {
        let c = config(SPLIT);
        assert_eq!(
            c.sessions[0].receive,
            Receive::Selective { people: vec!["Bob".into()], rooms: vec!["!family:silta.test".into()] }
        );
        assert_eq!(c.sessions[1].send, SendPolicy::Own);
    }

    #[test]
    fn reserved_keys_load_with_warnings() {
        let c = Config::parse(&format!(
            "{BASE}\n{}",
            r#"
[asr]
url = "http://127.0.0.1:5092/v1/"
[[sessions]]
name = "hub"
receive = "all"
send = "any"
user = "silta-hub"
relay_permissions = true
"#
        ))
        .expect("valid config");
        let w = c.warnings();
        assert_eq!(w.len(), 2, "{w:?}");
    }

    #[test]
    fn rejects_duplicates_and_ambiguity() {
        assert!(parse_err(
            r#"
[[people]]
name = "Alice"
role = "family"
addresses = ["@alice3:silta.test"]
"#
        )
        .contains("listed twice"));

        assert!(parse_err(
            r#"
[[people]]
name = "Carol"
role = "family"
addresses = ["@alice:silta.test"]
"#
        )
        .contains("address"));

        assert!(parse_err(
            r#"
[[sessions]]
name = "a"
receive = "all"
user = "u"
[[sessions]]
name = "b"
receive = "all"
user = "u"
"#
        )
        .contains("both have receive"));

        assert!(parse_err(
            r#"
[[sessions]]
name = "a"
receive = { rooms = ["!x:silta.test"] }
user = "u"
[[sessions]]
name = "b"
receive = { rooms = ["!x:silta.test"] }
user = "u"
"#
        )
        .contains("room"));

        assert!(parse_err(
            r#"
[[sessions]]
name = "a"
receive = { people = ["Alice"] }
user = "u"
[[sessions]]
name = "b"
receive = { people = ["Alice"] }
user = "u"
"#
        )
        .contains("person"));

        assert!(parse_err(
            r#"
[[sessions]]
name = "a"
receive = { people = ["Nobody"] }
user = "u"
"#
        )
        .contains("unknown person"));

        assert!(parse_err(
            r#"
[[sessions]]
name = "a"
receive = "all"
user = "u"
[[sessions]]
name = "a"
receive = { people = ["Alice"] }
user = "u"
"#
        )
        .contains("session"));

        assert!(matches!(
            Config::parse(&format!("{BASE}\n[[sessions]]\nname = \"a\"\nreceive = \"some\"\nuser = \"u\"\n")),
            Err(ConfigError::Parse { .. })
        ));
        // A session without a user cannot be checked at hello, so it is refused at start.
        assert!(matches!(
            Config::parse(&format!("{BASE}\n[[sessions]]\nname = \"a\"\nreceive = \"all\"\n")),
            Err(ConfigError::Parse { .. })
        ));
        assert!(parse_err(
            r#"
[[sessions]]
name = "a"
receive = "groups"
user = "u"
[[sessions]]
name = "b"
receive = "groups"
user = "u"
"#
        )
        .contains("both have receive"));
    }

    #[test]
    fn inbound_routing_precedence() {
        let r = Routing::new(&config(SPLIT));
        let bot = "@silta:silta.test";
        let dm = ["@alice:silta.test", bot];
        // Room beats person: Alice in the family room goes to the hub.
        assert_eq!(
            r.inbound("@alice:silta.test", "!family:silta.test", dm, PLAIN),
            Inbound::Deliver { session: "hub", person: &r.people[1] }
        );
        // Person otherwise, through any of their addresses.
        assert_eq!(
            r.inbound("@alice2:silta.test", "!dm:silta.test", ["@alice2:silta.test", bot], PLAIN),
            Inbound::Deliver { session: "alice", person: &r.people[1] }
        );
        assert_eq!(
            r.inbound("@bob:silta.test", "!dm2:silta.test", ["@bob:silta.test", bot], PLAIN),
            Inbound::Deliver { session: "hub", person: &r.people[0] }
        );
        assert_eq!(r.inbound("@mallory:silta.test", "!dm:silta.test", dm, PLAIN), Inbound::Drop(DropReason::UnknownSender));
        assert_eq!(r.inbound("@silta:silta.test", "!dm:silta.test", dm, PLAIN), Inbound::Drop(DropReason::OwnMessage));

        // Without an `all` session, an unlisted person is dropped.
        let r = Routing::new(&config(
            r#"
[[sessions]]
name = "alice"
receive = { people = ["Alice"] }
user = "u"
"#,
        ));
        assert_eq!(r.inbound("@bob:silta.test", "!x:silta.test", ["@bob:silta.test", bot], PLAIN), Inbound::Drop(DropReason::NoSession));

        // The `all` session is the fallback for everyone registered.
        let r = Routing::new(&config(HUB_ONLY));
        assert!(matches!(r.inbound("@bob:silta.test", "!any:silta.test", ["@bob:silta.test"], PLAIN), Inbound::Deliver { session: "hub", .. }));
        assert!(matches!(r.inbound("@alice:silta.test", "!any:silta.test", ["@alice:silta.test"], PLAIN), Inbound::Deliver { session: "hub", .. }));
        assert_eq!(r.people_for("hub").len(), 2);
    }

    #[test]
    fn group_rooms_go_to_the_hub_and_dms_to_the_minds() {
        let r = Routing::new(&config(MINDS));
        let bot = "@silta:silta.test";
        let family = ["@alice:silta.test", "@bob:silta.test", bot];
        let alice_dm = ["@alice:silta.test", bot];
        // Alice's message in the family room reaches the hub, in her DM her mind.
        assert!(matches!(r.inbound("@alice:silta.test", "!family:silta.test", family, PLAIN), Inbound::Deliver { session: "hub", .. }));
        assert!(matches!(r.inbound("@alice:silta.test", "!dm:silta.test", alice_dm, PLAIN), Inbound::Deliver { session: "alice", .. }));
        // Two addresses of one person are still a DM.
        assert!(matches!(
            r.inbound("@alice:silta.test", "!dm:silta.test", ["@alice:silta.test", "@alice2:silta.test", bot], PLAIN),
            Inbound::Deliver { session: "alice", .. }
        ));
        // A stranger in the room does not make it a group.
        assert!(matches!(
            r.inbound("@alice:silta.test", "!odd:silta.test", ["@alice:silta.test", "@mallory:silta.test", bot], PLAIN),
            Inbound::Deliver { session: "alice", .. }
        ));
        // The hub owns the family room and never a DM; a mind owns its DM only.
        assert_eq!(r.may_send("hub", "!family:silta.test", family, PLAIN), Ok(()));
        assert_eq!(r.may_read("hub", "!family:silta.test", family, PLAIN), Ok(()));
        assert_eq!(r.may_send("hub", "!dm:silta.test", alice_dm, PLAIN), Err(SendDenied::NotAllowed));
        assert_eq!(r.may_read("hub", "!dm:silta.test", alice_dm, PLAIN), Err(SendDenied::NotAllowed));
        assert_eq!(r.may_send("alice", "!dm:silta.test", alice_dm, PLAIN), Ok(()));
        assert_eq!(r.may_send("alice", "!family:silta.test", family, PLAIN), Err(SendDenied::NotAllowed));
        // A group room with a stranger belongs to nobody.
        assert_eq!(r.may_send("hub", "!odd:silta.test", ["@alice:silta.test", "@bob:silta.test", "@mallory:silta.test"], PLAIN), Err(SendDenied::NotAllowed));
        // Without a groups session a group room is dropped, unless an `all` session exists.
        let r = Routing::new(&config(SPLIT));
        assert!(matches!(r.inbound("@alice:silta.test", "!other:silta.test", family, PLAIN), Inbound::Drop(DropReason::NoSession)));
        // The welcome names the people a session receives from.
        let r = Routing::new(&config(MINDS));
        assert_eq!(r.people_for("alice").iter().map(|p| p.name.as_str()).collect::<Vec<_>>(), ["Alice"]);
        assert_eq!(r.people_for("hub").len(), 2);
        assert_eq!(r.people_for("bob").iter().map(|p| p.name.as_str()).collect::<Vec<_>>(), ["Bob"]);
        assert_eq!(r.session_users().collect::<Vec<_>>(), [("hub", "silta-hub"), ("bob", "silta-bob"), ("alice", "silta-alice")]);
    }

    #[test]
    fn the_dm_flag_and_the_name_decide_a_one_person_room() {
        let r = Routing::new(&config(MINDS));
        let bot = "@silta:silta.test";
        let bob_and_bot = ["@bob:silta.test", bot];
        let family = ["@alice:silta.test", "@bob:silta.test", bot];
        let bob = "@bob:silta.test";
        // A two-person room created as a room (named, no DM flag) is a group: the hub's.
        assert!(matches!(r.inbound(bob, "!pair:silta.test", bob_and_bot, RoomShape::NAMED), Inbound::Deliver { session: "hub", .. }));
        assert_eq!(r.may_send("hub", "!pair:silta.test", bob_and_bot, RoomShape::NAMED), Ok(()));
        assert_eq!(r.may_read("hub", "!pair:silta.test", bob_and_bot, RoomShape::NAMED), Ok(()));
        assert_eq!(r.may_send("bob", "!pair:silta.test", bob_and_bot, RoomShape::NAMED), Err(SendDenied::NotAllowed));
        assert_eq!(r.may_read("bob", "!pair:silta.test", bob_and_bot, RoomShape::NAMED), Err(SendDenied::NotAllowed));
        // A flagged DM is the person's, named or not.
        for shape in [RoomShape::DM, RoomShape { direct: true, named: true }] {
            assert!(matches!(r.inbound(bob, "!dm:silta.test", bob_and_bot, shape), Inbound::Deliver { session: "bob", .. }));
            assert_eq!(r.may_send("bob", "!dm:silta.test", bob_and_bot, shape), Ok(()));
            assert_eq!(r.may_send("hub", "!dm:silta.test", bob_and_bot, shape), Err(SendDenied::NotAllowed));
        }
        // An unflagged, unnamed two-person room is treated as a DM: the safe side.
        assert!(matches!(r.inbound(bob, "!bare:silta.test", bob_and_bot, PLAIN), Inbound::Deliver { session: "bob", .. }));
        // Two or more people are a group whatever the flags say.
        for shape in [PLAIN, RoomShape::DM, RoomShape::NAMED] {
            assert!(matches!(r.inbound(bob, "!family:silta.test", family, shape), Inbound::Deliver { session: "hub", .. }));
            assert_eq!(r.may_send("bob", "!family:silta.test", family, shape), Err(SendDenied::NotAllowed));
        }
        // A listed room goes where it is listed, whatever its shape.
        let r = Routing::new(&config(SPLIT));
        assert!(matches!(r.inbound("@alice:silta.test", "!family:silta.test", ["@alice:silta.test", bot], RoomShape::DM), Inbound::Deliver { session: "hub", .. }));
    }

    #[test]
    fn send_policy() {
        let r = Routing::new(&config(SPLIT));
        let bot = "@silta:silta.test";
        // `any` may write anywhere.
        assert_eq!(r.may_send("hub", "!whatever:silta.test", ["@mallory:silta.test", bot], PLAIN), Ok(()));
        // `own`: Alice's DM is fine, a room with Bob in it is not.
        assert_eq!(r.may_send("alice", "!dm:silta.test", ["@alice:silta.test", bot], PLAIN), Ok(()));
        assert_eq!(
            r.may_send("alice", "!family:silta.test", ["@alice:silta.test", "@bob:silta.test", bot], PLAIN),
            Err(SendDenied::NotAllowed)
        );
        assert_eq!(
            r.may_send("alice", "!x:silta.test", ["@alice:silta.test", "@mallory:silta.test"], PLAIN),
            Err(SendDenied::NotAllowed)
        );
        // A room the bot is alone in is not an own room either.
        assert_eq!(r.may_send("alice", "!empty:silta.test", [bot], PLAIN), Err(SendDenied::NotAllowed));
        assert_eq!(r.may_send("ghost", "!dm:silta.test", [bot], PLAIN), Err(SendDenied::UnknownSession));

        // `own` on an explicitly listed room ignores the member list.
        let r = Routing::new(&config(
            r#"
[[sessions]]
name = "fam"
receive = { rooms = ["!family:silta.test"] }
send = "own"
user = "u"
"#,
        ));
        assert_eq!(r.may_send("fam", "!family:silta.test", ["@mallory:silta.test"], PLAIN), Ok(()));
        assert_eq!(r.may_send("fam", "!other:silta.test", ["@alice:silta.test"], PLAIN), Err(SendDenied::NotAllowed));

        // An `all` session with `own` may write to any room of registered people.
        let r = Routing::new(&config(
            r#"
[[sessions]]
name = "hub"
receive = "all"
send = "own"
user = "u"
"#,
        ));
        assert_eq!(r.may_send("hub", "!x:silta.test", ["@alice:silta.test", "@bob:silta.test"], PLAIN), Ok(()));
        assert_eq!(r.may_send("hub", "!x:silta.test", ["@alice:silta.test", "@mallory:silta.test"], PLAIN), Err(SendDenied::NotAllowed));
    }

    #[test]
    fn read_policy_follows_ownership_not_send() {
        let r = Routing::new(&config(SPLIT));
        let bot = "@silta:silta.test";
        // The hub may write into Alice's DM (`any`) but not read it.
        assert_eq!(r.may_send("hub", "!dm:silta.test", ["@alice:silta.test", bot], PLAIN), Ok(()));
        assert_eq!(r.may_read("hub", "!dm:silta.test", ["@alice:silta.test", bot], PLAIN), Err(SendDenied::NotAllowed));
        // It reads the rooms it lists and the rooms of the people it receives from.
        assert_eq!(r.may_read("hub", "!family:silta.test", ["@alice:silta.test", "@bob:silta.test", bot], PLAIN), Ok(()));
        assert_eq!(r.may_read("hub", "!dm2:silta.test", ["@bob:silta.test", bot], PLAIN), Ok(()));
        // Alice's mind reads her DM only.
        assert_eq!(r.may_read("alice", "!dm:silta.test", ["@alice:silta.test", bot], PLAIN), Ok(()));
        assert_eq!(r.may_read("alice", "!family:silta.test", ["@alice:silta.test", "@bob:silta.test", bot], PLAIN), Err(SendDenied::NotAllowed));
        assert_eq!(r.may_read("ghost", "!dm:silta.test", [bot], PLAIN), Err(SendDenied::UnknownSession));

        // The `all` session reads every room, whoever is in it.
        let r = Routing::new(&config(HUB_ONLY));
        assert_eq!(r.may_read("hub", "!x:silta.test", ["@alice:silta.test", "@mallory:silta.test"], PLAIN), Ok(()));
    }
}
