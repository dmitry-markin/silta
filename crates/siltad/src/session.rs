//! Client construction and the one-time login versus session restore.

use std::{
    fs,
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, Context, Result};
use matrix_sdk::{
    authentication::matrix::MatrixSession,
    config::RequestConfig,
    encryption::{
        identities::Device, BackupDownloadStrategy, CrossSigningResetAuthType, EncryptionSettings,
    },
    ruma::api::{client::uiaa, error::ErrorKind},
    Client,
};
use tracing::{info, warn};

pub const SESSION_FILE: &str = "session.json";
pub const STORE_DIR: &str = "store";
/// Held while a process uses the store: two processes on one crypto store would each
/// keep the device's keys in memory and overwrite each other's. `backup` only reads.
pub const LOCK_FILE: &str = "siltad.lock";

pub struct Credentials<'a> {
    pub homeserver_url: &'a str,
    pub user_id: &'a str,
    pub password: &'a str,
    pub device_id: &'a str,
    pub device_name: &'a str,
}

/// Build the client with the sqlite store under `state_dir/store`. The configured
/// homeserver URL is authoritative: a server-advertised base URL is never followed.
///
/// Refuses to open a store that has no `session.json` next to it: the builder would
/// happily open it, and a fresh login onto an old encryption store corrupts it.
pub async fn build_client(homeserver_url: &str, state_dir: &StateDir) -> Result<Client> {
    let state_dir = state_dir.path();
    let session_file = state_dir.join(SESSION_FILE);
    let store_dir = state_dir.join(STORE_DIR);
    if !session_file.exists() && store_has_data(&store_dir) {
        bail!(
            "{} is missing but {} exists; a fresh login onto an old store would corrupt the \
             encryption state. Delete both {} and {} to start over.",
            session_file.display(),
            store_dir.display(),
            SESSION_FILE,
            STORE_DIR
        );
    }
    let client = Client::builder()
        .homeserver_url(homeserver_url)
        .respect_login_well_known(false)
        // The SDK's default retries a transient HTTP error (a 5xx from a reverse proxy, a
        // 429) for up to 15 minutes inside one request; a reply that hangs that long
        // blocks its session's socket for as long. A few attempts, then a `send_failed`
        // result the session can act on; the sync loop has its own retry.
        .request_config(RequestConfig::short_retry())
        .sqlite_store(state_dir.join(STORE_DIR), None)
        .with_encryption_settings(EncryptionSettings {
            auto_enable_cross_signing: true,
            auto_enable_backups: false,
            backup_download_strategy: BackupDownloadStrategy::Manual,
        })
        .build()
        .await
        .context("cannot build the Matrix client")?;
    Ok(client)
}

/// Restore `state_dir/session.json` if it exists, otherwise log in once with the fixed
/// device id and save the session. Refuses a login onto an existing store.
pub async fn login_or_restore(
    client: &Client,
    state_dir: &StateDir,
    creds: &Credentials<'_>,
) -> Result<()> {
    let session_file = state_dir.path().join(SESSION_FILE);

    if session_file.exists() {
        let text = fs::read_to_string(&session_file)
            .with_context(|| format!("cannot read {}", session_file.display()))?;
        let session: MatrixSession = serde_json::from_str(&text)
            .with_context(|| format!("cannot parse {}", session_file.display()))?;
        if session.meta.user_id != creds.user_id {
            bail!(
                "{} belongs to {} but the configuration says {}; delete {} and {} together to log in again",
                session_file.display(),
                session.meta.user_id,
                creds.user_id,
                SESSION_FILE,
                STORE_DIR
            );
        }
        if session.meta.device_id != creds.device_id {
            warn!(
                "saved session uses device {} while the configuration says {}; keeping the saved device",
                session.meta.device_id, creds.device_id
            );
        }
        client
            .restore_session(session)
            .await
            .context("cannot restore the saved session")?;
        // Confirm the token when the server is reachable. Only a rejected token is
        // fatal; an unreachable server is left to the sync loop's retries.
        match client.whoami().await {
            Ok(whoami) => info!(user = %whoami.user_id, device = ?whoami.device_id, "session restored"),
            Err(err) if is_token_error(&err) => bail!(
                "the server rejected the saved session ({err}); delete {} and {} together to log in again",
                SESSION_FILE,
                STORE_DIR
            ),
            Err(err) => warn!("session restored but the server could not confirm it yet: {err}"),
        }
        return Ok(());
    }

    info!(
        user = creds.user_id,
        device = creds.device_id,
        "no saved session, logging in"
    );
    let response = client
        .matrix_auth()
        .login_username(creds.user_id, creds.password)
        .device_id(creds.device_id)
        .initial_device_display_name(creds.device_name)
        .send()
        .await
        .context("login failed")?;
    if response.device_id != creds.device_id {
        warn!(
            "the server assigned device {} instead of the requested {}",
            response.device_id, creds.device_id
        );
    }
    if let Some(well_known) = &response.well_known {
        let advertised = well_known.homeserver.base_url.trim_end_matches('/');
        if advertised != creds.homeserver_url.trim_end_matches('/') {
            warn!(
                "the login response advertises {advertised} as the client API base URL; \
                 keeping the configured {}",
                creds.homeserver_url
            );
        }
    }

    client
        .encryption()
        .wait_for_e2ee_initialization_tasks()
        .await;

    let session = client
        .matrix_auth()
        .session()
        .context("no session after login")?;
    write_private(&session_file, &serde_json::to_string_pretty(&session)?)?;
    info!(user = %session.meta.user_id, device = %session.meta.device_id, "logged in, session saved to {}", session_file.display());
    Ok(())
}

/// The state directory, locked for as long as this value lives. [`build_client`] takes
/// it, so the store is never opened without the lock.
#[derive(Debug)]
pub struct StateDir {
    path: PathBuf,
    _lock: fs::File,
}

impl StateDir {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Create `state_dir` (mode 0700) if needed and lock it. Refuses another user's
/// directory: files created there (root's, say) would lock the daemon out of its store.
pub fn prepare_state_dir(state_dir: &Path) -> Result<StateDir> {
    fs::create_dir_all(state_dir)
        .with_context(|| format!("cannot create {}", state_dir.display()))?;
    let owner = fs::metadata(state_dir)
        .with_context(|| format!("cannot read {}", state_dir.display()))?
        .uid();
    let uid = nix::unistd::geteuid().as_raw();
    if owner != uid {
        bail!(
            "{} belongs to uid {owner} but this runs as uid {uid}; run it as the directory's owner",
            state_dir.display()
        );
    }
    fs::set_permissions(state_dir, fs::Permissions::from_mode(0o700))?;
    let path = state_dir.join(LOCK_FILE);
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)
        .with_context(|| format!("cannot open {}", path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(StateDir {
            path: state_dir.to_owned(),
            _lock: file,
        }),
        Err(fs::TryLockError::WouldBlock) => bail!(
            "another process (the daemon, say) holds {}; stop it first",
            path.display()
        ),
        Err(fs::TryLockError::Error(err)) => {
            Err(err).with_context(|| format!("cannot lock {}", path.display()))
        }
    }
}

/// Whether the homeserver shows this device signed by the account's cross-signing
/// identity: clients mark the messages of an unsigned device. The local status would
/// only say whether the private keys are in the store.
pub async fn device_signed_on_server(client: &Client) -> Result<bool> {
    let encryption = client.encryption();
    // The SDK's own key query and bootstrap after a login or restore.
    encryption.wait_for_e2ee_initialization_tasks().await;
    let user_id = client.user_id().context("not logged in")?;
    encryption
        .request_user_identity(user_id)
        .await
        .context("cannot query the account's keys")?;
    Ok(own_device(client).await?.is_cross_signed_by_owner())
}

async fn own_device(client: &Client) -> Result<Device> {
    client
        .encryption()
        .get_own_device()
        .await?
        .context("this device is missing from the store")
}

/// Log whether the device is cross-signed, and the fix when it is not.
pub async fn report_cross_signing(client: &Client) {
    match device_signed_on_server(client).await {
        Ok(true) => info!("this device is signed by the account's cross-signing identity"),
        Ok(false) => warn!(
            "this device is not signed by the account's cross-signing identity, so clients \
             mark its messages as sent by an unverified device; see \"Messages marked as sent \
             by an unverified device\" in docs/deployment.md"
        ),
        Err(err) => warn!("cannot check whether this device is cross-signed: {err:#}"),
    }
}

/// Sign this device with a cross-signing identity whose private keys are in the store,
/// replacing the account's identity if needed (when another client, such as Element,
/// created it). A replacement shows as an identity change to everyone who verified the
/// account, so a device that is already signed is left alone.
///
/// Keys left in the store by an interrupted run are tried first, so a rerun does not
/// replace the identity once more. The SDK drops stored keys that differ from the
/// server's when it fetches them, so a failure to sign with the rest is transient.
pub async fn reset_cross_signing(client: &Client, password: &str) -> Result<()> {
    if device_signed_on_server(client).await? {
        bail!(
            "this device is already signed by the account's cross-signing identity; a reset \
             would only make everyone who verified the assistant verify it again"
        );
    }
    if client
        .encryption()
        .cross_signing_status()
        .await
        .is_some_and(|status| status.is_complete())
    {
        own_device(client).await?.verify().await.context(
            "cannot sign this device with the stored cross-signing keys; run the command again",
        )?;
        if device_signed_on_server(client).await? {
            info!("this device is signed by the cross-signing identity of a previous reset");
            return Ok(());
        }
    }
    replace_identity(client, password).await?;
    if !device_signed_on_server(client).await? {
        bail!(
            "the homeserver does not show this device signed by the new cross-signing \
             identity; run the command again"
        );
    }
    info!("cross-signing reset; this device is signed by the new identity");
    Ok(())
}

/// Upload a new cross-signing identity, authorised with the configured password or, on a
/// homeserver with OAuth 2.0 accounts (matrix.org), by the owner in a browser.
async fn replace_identity(client: &Client, password: &str) -> Result<()> {
    let Some(handle) = client
        .encryption()
        .reset_cross_signing()
        .await
        .context("cannot reset cross-signing; run the command again")?
    else {
        return Ok(());
    };
    let (auth, oauth) = match handle.auth_type() {
        CrossSigningResetAuthType::Uiaa(info) => {
            let user_id = client.user_id().context("not logged in")?.to_owned();
            let mut password = uiaa::Password::new(user_id.into(), password.to_owned());
            password.session = info.session.clone();
            (uiaa::AuthData::Password(password), false)
        }
        CrossSigningResetAuthType::OAuth(info) => {
            // Not through tracing: a log filter must not hide what the owner has to do.
            eprintln!(
                "\nLogged in as the assistant's account, approve the reset of its \
                 cross-signing identity within two minutes at:\n\n    {}\n",
                info.approval_url
            );
            let mut oauth = uiaa::OAuth::new();
            oauth.session = info.session.clone();
            (uiaa::AuthData::OAuth(oauth), true)
        }
    };
    // The SDK retries every half second for two minutes until the server takes the
    // upload: the time to approve in the browser, but with a password each retry only
    // resends it to a server that wants another authentication stage.
    let auth = handle.auth(Some(auth));
    let result = if oauth {
        auth.await
    } else {
        tokio::time::timeout(Duration::from_secs(10), auth)
            .await
            .unwrap_or(Err(matrix_sdk::Error::Timeout))
    };
    match result {
        Ok(()) => Ok(()),
        Err(matrix_sdk::Error::Timeout) if oauth => {
            bail!("the reset was not approved within two minutes; run the command again")
        }
        Err(matrix_sdk::Error::Timeout) => bail!(
            "the homeserver did not accept the password within 10 seconds (it may want another \
             authentication step, which siltad cannot provide); run the command again"
        ),
        Err(err)
            if err
                .as_uiaa_response()
                .and_then(|info| info.auth_error.as_ref())
                .is_some_and(|error| error.kind == ErrorKind::Forbidden) =>
        {
            Err(err).context("the homeserver refused the password in siltad.toml")
        }
        Err(err) => Err(err).context(
            "the homeserver did not accept the new cross-signing keys; run the command again",
        ),
    }
}

/// M_UNKNOWN_TOKEN or M_MISSING_TOKEN: the saved session is dead.
pub fn is_token_error(err: &matrix_sdk::HttpError) -> bool {
    matches!(
        err.client_api_error_kind(),
        Some(ErrorKind::UnknownToken { .. } | ErrorKind::MissingToken)
    )
}

fn store_has_data(store_dir: &Path) -> bool {
    fs::read_dir(store_dir)
        .map(|mut entries| entries.next().is_some())
        .unwrap_or(false)
}

fn write_private(path: &Path, contents: &str) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("cannot write {}", path.display()))?;
    file.write_all(contents.as_bytes())?;
    Ok(())
}

/// Set the account's display name when the profile differs from the configuration. A
/// failure is logged, not fatal: the name is cosmetic and the daemon must still start.
pub async fn ensure_display_name(client: &Client, name: &str) {
    let account = client.account();
    match account.get_display_name().await {
        Ok(current) if current.as_deref() == Some(name) => {}
        Ok(current) => match account.set_display_name(Some(name)).await {
            Ok(()) => info!(from = ?current, "display name set to {name:?}"),
            Err(err) => warn!("cannot set the display name to {name:?}: {err}"),
        },
        Err(err) => warn!("cannot read the display name: {err}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_lock_of_the_state_dir_is_refused_until_the_first_is_dropped() {
        let dir = std::env::temp_dir().join(format!("siltad-lock-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let first = prepare_state_dir(&dir).unwrap();
        assert_eq!(first.path(), dir);
        let err = prepare_state_dir(&dir).unwrap_err().to_string();
        assert!(err.contains("another process"), "{err}");
        drop(first);
        prepare_state_dir(&dir).unwrap();
        fs::remove_dir_all(&dir).unwrap();
    }
}
