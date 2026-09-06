//! A small Matrix client for driving a test account against the loopback conduit.
//!
//! Environment: `TESTCLIENT_USER` and `TESTCLIENT_PASSWORD` (default: `ALICE_USER` and
//! `ALICE_PASSWORD` from `dev/state/accounts.env`), `TESTCLIENT_HOMESERVER` (default
//! `http://localhost`), `TESTCLIENT_STATE` (default `dev/state/testclient`),
//! `TESTCLIENT_DEVICE` (default `testclient`; give a concurrent second process its own
//! device and `TESTCLIENT_STATE`, two processes must not share one store). The store
//! and session live under `<state>/<localpart>`; files received by `watch` are saved
//! under `<state>/<localpart>/downloads`.
//!
//! Commands: `dm <user>` creates or finds the encrypted DM with a user; `room <name>
//! <user>...` creates an encrypted private room and invites users; `send <room> <text>`
//! sends a plain text message; `reply <room> <event_id> <text>` sends a quoted reply;
//! `thread <room> <root_event_id> <text>` sends a message in a thread; `emote <room>
//! <text>` sends a `/me` emote; `sendfile <room> <path> [caption]` uploads a file;
//! `react <room> <event_id> <emoji>` reacts; `edit <room> <event_id> <text>` replaces
//! an earlier message; `watch` prints decrypted incoming messages (attachments are
//! downloaded), reactions, edits and typing changes, and joins any invite.

use std::{env, path::PathBuf, process::ExitCode, time::Duration};

use anyhow::{bail, Context, Result};
use matrix_sdk::{
    attachment::AttachmentConfig,
    config::SyncSettings,
    event_handler::Ctx,
    media::{MediaFormat, MediaRequestParameters},
    ruma::{
        api::client::room::{create_room::v3::Request as CreateRoomRequest, Visibility},
        events::{
            reaction::{OriginalSyncReactionEvent, ReactionEventContent},
            relation::{Annotation, InReplyTo, Reply, Thread},
            room::{
                encrypted::OriginalSyncRoomEncryptedEvent,
                member::{MembershipState, StrippedRoomMemberEvent},
                message::{
                    MessageType, OriginalSyncRoomMessageEvent, Relation, ReplacementMetadata, RoomMessageEventContent,
                    TextMessageEventContent,
                },
                MediaSource,
            },
            typing::SyncTypingEvent,
            MessageLikeEventContent,
        },
        EventId, OwnedUserId, RoomId, UserId,
    },
    Client, Room, RoomState,
};
use siltad::session::{build_client, login_or_restore, Credentials};

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("testclient: {err:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let user = env::var("TESTCLIENT_USER").or_else(|_| env::var("ALICE_USER")).context("set TESTCLIENT_USER or ALICE_USER")?;
    let password = env::var("TESTCLIENT_PASSWORD").or_else(|_| env::var("ALICE_PASSWORD")).context("set TESTCLIENT_PASSWORD or ALICE_PASSWORD")?;
    let homeserver = env::var("TESTCLIENT_HOMESERVER").unwrap_or_else(|_| "http://localhost".into());
    let state_root = env::var("TESTCLIENT_STATE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../dev/state/testclient"));
    let device = env::var("TESTCLIENT_DEVICE").unwrap_or_else(|_| "testclient".into());
    let user_id = UserId::parse(&user).context("TESTCLIENT_USER is not a Matrix user id")?;
    let state_dir = state_root.join(user_id.localpart());

    let client = build_client(&homeserver, &state_dir).await?;
    login_or_restore(
        &client,
        &state_dir,
        &Credentials {
            homeserver_url: &homeserver,
            user_id: &user,
            password: &password,
            device_id: &device,
            device_name: &format!("silta {device}"),
        },
    )
    .await?;

    match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["dm", other] => dm(&client, other).await,
        ["room", name, users @ ..] if !users.is_empty() => room(&client, name, users).await,
        ["send", room_id, text @ ..] if !text.is_empty() => {
            send(&client, room_id, RoomMessageEventContent::text_plain(text.join(" "))).await
        }
        ["reply", room_id, event_id, text @ ..] if !text.is_empty() => {
            let mut content = RoomMessageEventContent::text_plain(text.join(" "));
            content.relates_to = Some(Relation::Reply(Reply::new(InReplyTo::new(EventId::parse(event_id)?))));
            send(&client, room_id, content).await
        }
        ["thread", room_id, root, text @ ..] if !text.is_empty() => {
            let root = EventId::parse(root)?;
            let mut content = RoomMessageEventContent::text_plain(text.join(" "));
            content.relates_to = Some(Relation::Thread(Thread::plain(root.clone(), root)));
            send(&client, room_id, content).await
        }
        ["emote", room_id, text @ ..] if !text.is_empty() => {
            send(&client, room_id, RoomMessageEventContent::emote_plain(text.join(" "))).await
        }
        ["sendfile", room_id, path, caption @ ..] => sendfile(&client, room_id, path, caption.join(" ")).await,
        ["react", room_id, event_id, emoji] => {
            send(&client, room_id, ReactionEventContent::new(Annotation::new(EventId::parse(event_id)?, (*emoji).to_owned()))).await
        }
        ["edit", room_id, event_id, text @ ..] if !text.is_empty() => {
            let content = RoomMessageEventContent::text_plain(text.join(" "))
                .make_replacement(ReplacementMetadata::new(EventId::parse(event_id)?, None));
            send(&client, room_id, content).await
        }
        ["watch"] => watch(&client, state_dir.join("downloads")).await,
        _ => bail!(
            "usage: testclient dm <user> | room <name> <user>... | send <room> <text> | reply <room> <event_id> <text> | \
             thread <room> <root_event_id> <text> | emote <room> <text> | sendfile <room> <path> [caption] | \
             react <room> <event_id> <emoji> | edit <room> <event_id> <text> | watch"
        ),
    }
}

async fn sync_once(client: &Client) -> Result<()> {
    client.sync_once(SyncSettings::default().timeout(Duration::from_secs(5))).await?;
    Ok(())
}

async fn dm(client: &Client, other: &str) -> Result<()> {
    let other = UserId::parse(other)?;
    sync_once(client).await?;
    let room = match client.get_dm_room(&other) {
        Some(room) => {
            eprintln!("existing DM found");
            room
        }
        None => {
            eprintln!("creating an encrypted DM with {other}");
            client.create_dm(&other).await?
        }
    };
    if !room.latest_encryption_state().await?.is_encrypted() {
        eprintln!("enabling encryption");
        room.enable_encryption().await?;
    }
    println!("{}", room.room_id());
    Ok(())
}

async fn room(client: &Client, name: &str, users: &[&str]) -> Result<()> {
    let invites: Vec<OwnedUserId> = users.iter().map(UserId::parse).collect::<Result<_, _>>()?;
    sync_once(client).await?;
    let mut request = CreateRoomRequest::new();
    request.name = Some(name.to_owned());
    request.visibility = Visibility::Private;
    request.invite = invites;
    let room = client.create_room(request).await?;
    room.enable_encryption().await?;
    println!("{}", room.room_id());
    Ok(())
}

async fn joined_room(client: &Client, room_id: &str) -> Result<Room> {
    let room_id = RoomId::parse(room_id)?;
    sync_once(client).await?;
    let room = client.get_room(&room_id).context("not in that room (run watch to accept invites)")?;
    if room.state() != RoomState::Joined {
        bail!("not joined to {room_id}");
    }
    Ok(room)
}

async fn send(client: &Client, room_id: &str, content: impl MessageLikeEventContent) -> Result<()> {
    let room = joined_room(client, room_id).await?;
    let response = room.send(content).await?;
    println!("{}", response.response.event_id);
    Ok(())
}

async fn sendfile(client: &Client, room_id: &str, path: &str, caption: String) -> Result<()> {
    let path = PathBuf::from(path);
    let data = std::fs::read(&path).with_context(|| format!("cannot read {}", path.display()))?;
    let name = path.file_name().and_then(|n| n.to_str()).context("bad file name")?.to_owned();
    let mime = mime_guess::from_path(&path).first_or_octet_stream();
    let mut config = AttachmentConfig::new();
    if !caption.trim().is_empty() {
        config = config.caption(Some(TextMessageEventContent::plain(caption)));
    }
    let room = joined_room(client, room_id).await?;
    let response = room.send_attachment(name, &mime, data, config).await?;
    println!("{}", response.event_id);
    Ok(())
}

async fn watch(client: &Client, downloads: PathBuf) -> Result<()> {
    std::fs::create_dir_all(&downloads)?;
    client.add_event_handler_context(downloads);
    client.add_event_handler(|event: StrippedRoomMemberEvent, room: Room, client: Client| async move {
        if Some(event.state_key.as_ref()) == client.user_id() && event.content.membership == MembershipState::Invite {
            eprintln!("invite to {} from {}, joining", room.room_id(), event.sender);
            if let Err(err) = room.join().await {
                eprintln!("join failed: {err}");
            }
        }
    });
    client.add_event_handler(|event: OriginalSyncRoomMessageEvent, room: Room, client: Client, Ctx(downloads): Ctx<PathBuf>| async move {
        if room.state() != RoomState::Joined {
            return;
        }
        let relation = match &event.content.relates_to {
            Some(Relation::Reply(r)) => format!(" (reply to {})", r.in_reply_to.event_id),
            Some(Relation::Thread(t)) => format!(" (thread {})", t.event_id),
            Some(Relation::Replacement(r)) => format!(" (edit of {})", r.event_id),
            _ => String::new(),
        };
        let body = match &event.content.msgtype {
            MessageType::Text(text) => text.body.clone(),
            MessageType::Emote(emote) => format!("/me {}", emote.body),
            MessageType::Image(c) => media(&client, &downloads, "image", &c.body, c.filename.as_deref(), &c.source).await,
            MessageType::File(c) => media(&client, &downloads, "file", &c.body, c.filename.as_deref(), &c.source).await,
            MessageType::Audio(c) => media(&client, &downloads, "audio", &c.body, c.filename.as_deref(), &c.source).await,
            MessageType::Video(c) => media(&client, &downloads, "video", &c.body, c.filename.as_deref(), &c.source).await,
            other => format!("<{}>", other.msgtype()),
        };
        println!("[{}] {} {}{}: {}", room.room_id(), event.event_id, event.sender, relation, body.replace('\n', "\\n"));
    });
    client.add_event_handler(|event: OriginalSyncReactionEvent, room: Room| async move {
        println!(
            "[{}] {} {} reaction {} on {}",
            room.room_id(),
            event.event_id,
            event.sender,
            event.content.relates_to.key,
            event.content.relates_to.event_id
        );
    });
    client.add_event_handler(|event: OriginalSyncRoomEncryptedEvent, room: Room| async move {
        println!("[{}] {} {}: <undecryptable>", room.room_id(), event.event_id, event.sender);
    });
    client.add_event_handler(|event: SyncTypingEvent, room: Room| async move {
        let who: Vec<String> = event.content.user_ids.iter().map(|u| u.to_string()).collect();
        let now = silta::time::rfc3339_utc(
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0),
        );
        println!("[{}] {} typing: {}", room.room_id(), &now[11..19], if who.is_empty() { "(none)".to_owned() } else { who.join(", ") });
    });
    eprintln!("watching as {} (Ctrl-C to stop)", client.user_id().map(|u| u.to_string()).unwrap_or_default());
    client.sync(SyncSettings::default().timeout(Duration::from_secs(30))).await?;
    Ok(())
}

/// Download an attachment into the downloads directory and describe it.
async fn media(client: &Client, downloads: &std::path::Path, kind: &str, body: &str, filename: Option<&str>, source: &MediaSource) -> String {
    let (name, caption) = match filename {
        Some(f) if f != body => (f, Some(body)),
        Some(f) => (f, None),
        None => (body, None),
    };
    let safe: String = name.chars().map(|c| if c == '/' || c.is_control() { '_' } else { c }).collect();
    let path = downloads.join(&safe);
    let request = MediaRequestParameters { source: source.clone(), format: MediaFormat::File };
    let saved = match client.media().get_media_content(&request, false).await {
        Ok(data) => match std::fs::write(&path, &data) {
            Ok(()) => format!("{} bytes saved to {}", data.len(), path.display()),
            Err(err) => format!("cannot save: {err}"),
        },
        Err(err) => format!("download failed: {err}"),
    };
    let caption = caption.map(|c| format!(" caption: {c}")).unwrap_or_default();
    format!("<{kind} {name}: {saved}>{caption}")
}
