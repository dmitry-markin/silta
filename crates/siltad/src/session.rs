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
    match client.encryption().cross_signing_status().await {
        Some(status) if status.is_complete() => info!("cross-signing bootstrapped"),
        status => warn!(
            "cross-signing is not complete ({status:?}); the device works unsigned. If the \
             account's cross-signing was set up by another client, stop siltad and run \
             `siltad reset-cross-signing`"
        ),
    }

    let session = client
        .matrix_auth()
        .session()
        .context("no session after login")?;
    write_private(&session_file, &serde_json::to_string_pretty(&session)?)?;
    info!(user = %session.meta.user_id, device = %session.meta.device_id, "logged in, session saved to {}", session_file.display());
    Ok(())
}

/// Replace the account's cross-signing identity with a new one whose private keys stay
/// in this device's store, and sign this device with it. For an account whose identity
/// another client created (Element does at registration): without its private keys the
/// device works unsigned, and clients mark its messages as sent by an unverified device.
/// The other sessions of the account and the people who verified it see the identity
/// change.
///
/// A homeserver with password UIA takes the configured password. One with OAuth 2.0
/// accounts (matrix.org) asks the owner to approve the reset in a browser; the SDK
/// retries the upload for two minutes while waiting for the approval.
pub async fn reset_cross_signing(client: &Client, password: &str) -> Result<()> {
    let encryption = client.encryption();
    encryption.wait_for_e2ee_initialization_tasks().await;
    let Some(handle) = encryption
        .reset_cross_signing()
        .await
        .context("cannot reset cross-signing")?
    else {
        info!("the homeserver accepted the new cross-signing keys without authentication");
        return check_cross_signing(client).await;
    };
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
            warn!(
                "approve the cross-signing reset within two minutes at {}",
                info.approval_url
            );
            let mut oauth = uiaa::OAuth::new();
            oauth.session = info.session.clone();
            uiaa::AuthData::OAuth(oauth)
        }
    };
    handle
        .auth(Some(auth))
        .await
        .context("the homeserver did not accept the new cross-signing keys")?;
    check_cross_signing(client).await
}

async fn check_cross_signing(client: &Client) -> Result<()> {
    match client.encryption().cross_signing_status().await {
        Some(status) if status.is_complete() => {
            info!("cross-signing reset; this device is signed by the new identity");
            Ok(())
        }
        status => bail!("cross-signing is still not complete after the reset ({status:?})"),
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
