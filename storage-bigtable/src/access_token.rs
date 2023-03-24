pub use goauth::scopes::Scope;
use gcp_auth::{AuthenticationManager,Token};
use tokio::sync::OnceCell;

/// A module for managing a Google API access token
use {
    log::*,
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            {Arc, RwLock},
        },
        time::Instant,
    },
    time::OffsetDateTime,
};

static AUTH_MANAGER: OnceCell<AuthenticationManager> = OnceCell::const_new();

async fn authentication_manager() -> &'static AuthenticationManager {
    AUTH_MANAGER
        .get_or_init(|| async {
            AuthenticationManager::new()
                .await
                .expect("unable to initialize authentication manager")
        })
        .await
}

#[derive(Clone)]
pub struct AccessToken {
    scope: Scope,
    refresh_active: Arc<AtomicBool>,
    token: Arc<RwLock<(Token, Instant)>>,
}

impl AccessToken {
    pub async fn new(scope: Scope) -> Result<Self, String> {
        let token = Arc::new(RwLock::new(Self::get_token(&scope).await?));
        let access_token = Self {
            scope,
            token,
            refresh_active: Arc::new(AtomicBool::new(false)),
        };
        Ok(access_token)
    }

    fn expires_in(token: &Token) -> i64 {
        token.expires_at().unwrap().unix_timestamp() - OffsetDateTime::now_utc().unix_timestamp()
    }

    async fn get_token(
        scope: &Scope,
    ) -> Result<(Token, Instant), String> {        
        let authentication_manager = authentication_manager().await;
        let scope_url = scope.url();
        let scopes = &[scope_url.as_str()];
        let token = authentication_manager.get_token(scopes).await
            .map_err(|err| format!("Unable to get token: {}", err))?;

        info!("Token expires in {} seconds", Self::expires_in(&token));
        Ok((token, Instant::now()))
    }

    /// Call this function regularly to ensure the access token does not expire
    pub async fn refresh(&self) {
        // Check if it's time to try a token refresh
        {
            let token_r = self.token.read().unwrap();
            if token_r.1.elapsed().as_secs() < Self::expires_in(&token_r.0) as u64 / 2 {
                return;
            }

            #[allow(deprecated)]
            if self
                .refresh_active
                .compare_and_swap(false, true, Ordering::Relaxed)
            {
                // Refresh already pending
                return;
            }
        }

        info!("Refreshing token");
        match tokio::time::timeout(
            tokio::time::Duration::from_secs(5),
            Self::get_token(&self.scope),
        )
        .await
        {
            Ok(new_token) => match (new_token, self.token.write()) {
                (Ok(new_token), Ok(mut token_w)) => *token_w = new_token,
                (Ok(_new_token), Err(err)) => warn!("{}", err),
                (Err(err), _) => warn!("{}", err),
            },
            Err(_) => {
                warn!("Token refresh timeout")
            }
        }
        self.refresh_active.store(false, Ordering::Relaxed);
    }

    /// Return an access token suitable for use in an HTTP authorization header
    pub fn get(&self) -> String {
        let token_r = self.token.read().unwrap();
        format!("Bearer {}", token_r.0.as_str())
    }
}
