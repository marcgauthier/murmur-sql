//! Postgres-wire authentication: cleartext password request with Argon2
//! verification against `auth.users` from `config.toml`.
//!
//! The password crosses the wire in cleartext (inside the PG `PasswordMessage`
//! flow), exactly like Postgres `password` authentication. Use TLS in
//! production; `server.production` enforces TLS with client certificates.

use crate::server::ServerState;
use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use pgwire::api::auth::{
    LoginInfo, ServerParameterProvider, StartupHandler, finish_authentication,
    protocol_negotiation, save_startup_parameters_to_metadata,
};
use pgwire::api::{
    ClientInfo, PgWireConnectionState, PidSecretKeyGenerator, RandomPidSecretKeyGenerator,
};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use std::fmt::Debug;
use std::sync::Arc;

/// Hash a password for `auth.users` entries. Output is a PHC string
/// (`$argon2id$...`). Used by `murmur-admin hash-password` and tests.
pub fn hash_password(password: &str) -> Result<String, String> {
    use password_hash::rand_core::OsRng;
    use password_hash::{PasswordHasher, SaltString};
    let salt = SaltString::generate(&mut OsRng);
    argon2::Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|e| format!("cannot hash password: {e}"))
}

/// Verify a candidate password against a stored PHC hash. Unknown or
/// malformed hashes never authenticate.
pub fn verify_password(hash: &str, password: &[u8]) -> bool {
    use password_hash::{PasswordHash, PasswordVerifier};
    let Ok(parsed) = PasswordHash::new(hash) else {
        return false;
    };
    argon2::Argon2::default()
        .verify_password(password, &parsed)
        .is_ok()
}

/// Startup handler requesting a cleartext password and verifying it with
/// Argon2. Unknown users are rejected before the password round-trip.
pub struct Argon2StartupHandler<P> {
    state: Arc<ServerState>,
    parameter_provider: P,
    production: bool,
    pid_secret_key_generator: Arc<dyn PidSecretKeyGenerator>,
}

impl<P> Argon2StartupHandler<P>
where
    P: ServerParameterProvider,
{
    pub fn new(state: Arc<ServerState>, parameter_provider: P, production: bool) -> Self {
        Self {
            state,
            parameter_provider,
            production,
            pid_secret_key_generator: Arc::new(RandomPidSecretKeyGenerator::default()),
        }
    }
}

#[async_trait]
impl<P> StartupHandler for Argon2StartupHandler<P>
where
    P: ServerParameterProvider,
{
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                // pgwire negotiates down to plaintext when the client skips
                // SSLRequest; production refuses that downgrade. Client
                // certificates are already enforced by the TLS handshake.
                if self.production && !client.is_secure() {
                    tracing::warn!(peer = %client.socket_addr(), "production connection rejected: TLS required");
                    return Err(crate::server::query::user_error(
                        "28000",
                        "production mode requires a TLS connection".to_string(),
                    ));
                }
                let state = self.state.clone();
                let peer = client.socket_addr();
                tokio::task::spawn_blocking(move || state.disconnect(peer))
                    .await
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                protocol_negotiation(client, startup).await?;
                save_startup_parameters_to_metadata(client, startup);
                let login = LoginInfo::from_client_info(client);
                let user = login.user().unwrap_or_default();
                if self.state.user_hash(user).is_none() {
                    tracing::warn!(user = %user, peer = %client.socket_addr(), "wire login rejected: unknown user");
                    return Err(PgWireError::InvalidPassword(user.to_owned()));
                }
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(message) => {
                let message = message.into_password()?;
                let login = LoginInfo::from_client_info(client);
                let user = login.user().unwrap_or_default().to_owned();
                let expected = self.state.user_hash(&user);
                let candidate = message.password.as_bytes().to_vec();
                // Argon2 is CPU-heavy; keep it off the async worker.
                let verified = tokio::task::spawn_blocking(move || {
                    expected.is_some_and(|hash| verify_password(&hash, &candidate))
                })
                .await
                .unwrap_or(false);
                if verified {
                    tracing::info!(user = %user, peer = %client.socket_addr(), "wire login accepted");
                    let (pid, secret_key) = self.pid_secret_key_generator.generate(client);
                    client.set_pid_and_secret_key(pid, secret_key);
                    finish_authentication(client, &self.parameter_provider).await?;
                } else {
                    tracing::warn!(user = %user, peer = %client.socket_addr(), "wire login rejected: bad password");
                    return Err(PgWireError::InvalidPassword(user));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// Handler factory handed to `pgwire::tokio::process_socket`.
pub struct HandlerFactory {
    pub state: Arc<ServerState>,
    pub production: bool,
}

impl pgwire::api::PgWireServerHandlers for HandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl pgwire::api::query::SimpleQueryHandler> {
        Arc::new(crate::server::query::QueryBackend::new(self.state.clone()))
    }

    fn extended_query_handler(&self) -> Arc<impl pgwire::api::query::ExtendedQueryHandler> {
        Arc::new(crate::server::query::QueryBackend::new(self.state.clone()))
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        let mut parameters = pgwire::api::auth::DefaultServerParameterProvider::default();
        parameters.server_version = crate::server::PG_SERVER_VERSION.to_owned();
        Arc::new(Argon2StartupHandler::new(
            self.state.clone(),
            parameters,
            self.production,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_hash_round_trip() {
        let hash = hash_password("correct horse").unwrap();
        assert!(hash.starts_with("$argon2id$"));
        assert!(verify_password(&hash, b"correct horse"));
        assert!(!verify_password(&hash, b"wrong"));
        assert!(!verify_password("not-a-hash", b"correct horse"));
        assert!(!verify_password("", b""));
    }
}
