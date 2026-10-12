//! Client construction and the one-time login versus session restore.

use std::{
    fs,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
};

use anyhow::{bail, Context, Result};
use matrix_sdk::{
    authentication::matrix::MatrixSession,
    config::RequestConfig,
    encryption::{BackupDownloadStrategy, CrossSigningResetAuthType, EncryptionSettings},
    ruma::api::{client::uiaa, error::ErrorKind},
    Client,
};
use tracing::{info, warn};

pub const SESSION_FILE: &str = "session.json";
pub const STORE_DIR: &str = "store";
/// Held by the daemon and by `reset-cross-signing` while they use the store: two
/// processes on one crypto store each keep their own copy of the device's keys in
/// memory and overwrite each other's. `backup` only reads and does not take it.
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
pub async fn build_client(homeserver_url: &str, state_dir: &Path) -> Result<Client> {
    fs::create_dir_all(state_dir)
        .with_context(|| format!("cannot create {}", state_dir.display()))?;
    fs::set_permissions(state_dir, fs::Permissions::from_mode(0o700))?;
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
    state_dir: &Path,
    creds: &Credentials<'_>,
) -> Result<()> {
    let session_file = state_dir.join(SESSION_FILE);
    let store_dir = state_dir.join(STORE_DIR);

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

    // The store-without-session case was refused in `build_client`, before the builder
    // created the (then empty) store.
    let _ = store_dir;

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

/// Take the lock of `state_dir`, creating both if needed; the lock lasts as long as
/// the returned file.
pub fn lock_state_dir(state_dir: &Path) -> Result<fs::File> {
    fs::create_dir_all(state_dir)
        .with_context(|| format!("cannot create {}", state_dir.display()))?;
    let path = state_dir.join(LOCK_FILE);
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)
        .with_context(|| format!("cannot open {}", path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(fs::TryLockError::WouldBlock) => bail!(
            "another siltad process (the daemon, or a reset-cross-signing in progress) holds \
             {}; stop it first",
            path.display()
        ),
        Err(fs::TryLockError::Error(err)) => {
            Err(err).with_context(|| format!("cannot lock {}", path.display()))
        }
    }
}

/// Whether the homeserver shows this device signed by the account's self-signing key,
/// which is what clients check before marking its messages as sent by a device not
/// verified by its owner. Asks the server: the local cross-signing status only says
/// whether the private keys are in the store.
pub async fn device_signed_on_server(client: &Client) -> Result<bool> {
    let encryption = client.encryption();
    let user_id = client.user_id().context("not logged in")?;
    encryption
        .request_user_identity(user_id)
        .await
        .context("cannot query the account's keys")?;
    let device = encryption
        .get_own_device()
        .await?
        .context("this device is missing from the store")?;
    Ok(device.is_cross_signed_by_owner())
}

/// Log whether the device is cross-signed, and how to fix it when it is not. Not fatal:
/// an unsigned device works, its messages are only marked.
pub async fn report_cross_signing(client: &Client) {
    match device_signed_on_server(client).await {
        Ok(true) => info!("this device is signed by the account's cross-signing identity"),
        Ok(false) => warn!(
            "this device is not signed by the account's cross-signing identity, so clients \
             mark its messages as sent by an unverified device; when another client set up \
             the account's cross-signing, stop siltad and run `siltad reset-cross-signing`"
        ),
        Err(err) => warn!("cannot check whether this device is cross-signed: {err:#}"),
    }
}

/// Replace the account's cross-signing identity with a new one whose private keys stay
/// in this device's store, and sign this device with it. For an account whose identity
/// another client created (Element does at registration): without its private keys the
/// device works unsigned, and clients mark its messages as sent by an unverified device.
/// The other sessions of the account and the people who verified it see the identity
/// change, so a device the server already shows as signed is left alone.
///
/// A homeserver with password UIA takes the configured password. One with OAuth 2.0
/// accounts (matrix.org) asks the owner to approve the reset in a browser; the SDK
/// retries the upload for two minutes while waiting for the approval.
///
/// The SDK saves the new private keys before the upload is authorised, so a failed or
/// interrupted reset leaves keys in the store that the server never accepted. Running
/// the reset again replaces them; until then the device works unsigned as before.
pub async fn reset_cross_signing(client: &Client, password: &str) -> Result<()> {
    let encryption = client.encryption();
    encryption.wait_for_e2ee_initialization_tasks().await;
    if device_signed_on_server(client).await? {
        bail!(
            "this device is already signed by the account's cross-signing identity; a reset \
             would only make everyone who verified the assistant verify it again"
        );
    }
    let handle = encryption
        .reset_cross_signing()
        .await
        .context("cannot reset cross-signing; run the command again")?;
    if let Some(handle) = handle {
        let auth = match handle.auth_type() {
            CrossSigningResetAuthType::Uiaa(info) => {
                let user_id = client
                    .user_id()
                    .context("no user id after login")?
                    .to_owned();
                let mut password = uiaa::Password::new(user_id.into(), password.to_owned());
                password.session = info.session.clone();
                uiaa::AuthData::Password(password)
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
                uiaa::AuthData::OAuth(oauth)
            }
        };
        match handle.auth(Some(auth)).await {
            Ok(()) => {}
            Err(matrix_sdk::Error::Timeout) => {
                bail!("the reset was not approved within two minutes; run the command again")
            }
            Err(err) => {
                return Err(err).context(
                    "the homeserver did not accept the new cross-signing keys; run the command \
                     again",
                )
            }
        }
    }
    if !device_signed_on_server(client).await? {
        bail!(
            "the homeserver does not show this device signed by the new cross-signing \
             identity; run the command again"
        );
    }
    info!("cross-signing reset; this device is signed by the new identity");
    Ok(())
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
        let first = lock_state_dir(&dir).unwrap();
        let err = lock_state_dir(&dir).unwrap_err().to_string();
        assert!(err.contains("another siltad process"), "{err}");
        drop(first);
        lock_state_dir(&dir).unwrap();
        fs::remove_dir_all(&dir).unwrap();
    }
}
